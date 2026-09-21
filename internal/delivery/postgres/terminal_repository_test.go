package postgres_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/reconciliation"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

type terminalInspector func(context.Context, workerclient.JobID) (workerclient.Job, error)

func (f terminalInspector) Inspect(ctx context.Context, id workerclient.JobID) (workerclient.Job, error) {
	return f(ctx, id)
}

// The admin URL only creates/drops a randomly named disposable database, matching
// the existing repository integration fixtures. No application database is reset.
func TestTerminalRepositoryIntegration(t *testing.T) {
	dsn := os.Getenv("IRIS_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("IRIS_TEST_DATABASE_URL unset; isolated PostgreSQL checks skipped")
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
	name := "iris_terminal_test_" + hex.EncodeToString(suffix[:])
	quoted := pgx.Identifier{name}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quoted); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(cleanup, "DROP DATABASE "+quoted); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	paths, err := filepath.Glob("../../../migrations/*.up.sql")
	if err != nil || len(paths) == 0 {
		t.Fatal("missing migrations")
	}
	for _, path := range paths {
		sql, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatal(err)
		}
	}
	at := time.Date(2026, 9, 21, 0, 0, 0, 123456000, time.UTC)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, sql := range []string{`INSERT INTO events VALUES ('event','type',$1,'{}')`, `INSERT INTO endpoints VALUES ('endpoint','https://example.com',$1,false)`, `INSERT INTO deliveries VALUES ('delivery','event','endpoint',$1)`} {
		_, err := pool.Exec(ctx, sql, at)
		must(err)
	}
	seed := func(id string, when time.Time, submitted bool) {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO delivery_runs VALUES ($1,'delivery',$2,'manual_redelivery')`, id, when)
		must(err)
		var job, stamp any
		if submitted {
			job = "job-" + id
			stamp = when
		}
		_, err = pool.Exec(ctx, `INSERT INTO outbox (run_id,submission_key,created_at,mercury_job_id,submitted_at) VALUES ($1,$1,$2,$3,$4)`, id, when, job, stamp)
		must(err)
	}
	value := func(id string, state delivery.TerminalState, when time.Time) delivery.RunTerminal {
		t.Helper()
		v, err := delivery.NewRunTerminal(delivery.RunID(id), "job-"+id, state, when)
		must(err)
		return v
	}
	repo := deliverypg.NewTerminalRepository(pool)
	seed("A", at, true)
	seed("z", at, true)
	seed("later", at.Add(time.Minute), true)
	seed("pending", at, false)

	t.Run("deterministic bounded paging excludes pending", func(t *testing.T) {
		page, err := repo.ListUnresolvedAfter(ctx, 2, time.Time{}, "")
		must(err)
		if len(page) != 2 || page[0].Run.ID() != "A" || page[1].Run.ID() != "z" {
			t.Fatalf("first page: %+v", page)
		}
		next, err := repo.ListUnresolvedAfter(ctx, 2, page[1].Run.CreatedAt(), page[1].Run.ID())
		must(err)
		if len(next) != 1 || next[0].Run.ID() != "later" {
			t.Fatalf("next page: %+v", next)
		}
		last, err := repo.ListUnresolvedAfter(ctx, 2, next[0].Run.CreatedAt(), next[0].Run.ID())
		must(err)
		if len(last) != 0 {
			t.Fatal("unexpected last page")
		}
		for _, limit := range []int{0, -1, 1001} {
			if _, err := repo.ListUnresolvedAfter(ctx, limit, time.Time{}, ""); err == nil {
				t.Fatal("invalid limit accepted")
			}
		}
	})

	t.Run("round trips replay conflict and unknown attempts", func(t *testing.T) {
		start, err := delivery.NewAttemptStart("unknown", "A", at)
		must(err)
		attempts := deliverypg.NewAttemptRepository(pool)
		must(attempts.AppendStart(ctx, start))
		first := value("A", delivery.TerminalFailed, at.Add(time.Hour))
		must(repo.Record(ctx, first))
		must(repo.Record(ctx, value("A", delivery.TerminalFailed, at.Add(2*time.Hour))))
		got, err := repo.GetByRunID(ctx, "A")
		must(err)
		if got.RunID() != first.RunID() || got.MercuryJobID() != first.MercuryJobID() || got.State() != first.State() || !got.ObservedAt().Equal(first.ObservedAt()) {
			t.Fatal("replay changed first observation")
		}
		if err := repo.Record(ctx, value("A", delivery.TerminalSucceeded, at)); !errors.Is(err, deliverypg.ErrTerminalConflict) {
			t.Fatalf("state conflict: %v", err)
		}
		wrong, err := delivery.NewRunTerminal("A", "other-job", delivery.TerminalFailed, at)
		must(err)
		if err := repo.Record(ctx, wrong); !errors.Is(err, deliverypg.ErrTerminalConflict) {
			t.Fatalf("job conflict: %v", err)
		}
		history, err := attempts.GetByID(ctx, "unknown")
		must(err)
		if history.Start.ID() != start.ID() || history.Start.RunID() != start.RunID() || !history.Start.StartedAt().Equal(start.StartedAt()) || history.Outcome != nil {
			t.Fatal("terminal job fabricated attempt outcome")
		}
		if _, err := repo.GetByRunID(ctx, "z"); !errors.Is(err, deliverypg.ErrTerminalNotFound) {
			t.Fatalf("older run altered another run: %v", err)
		}
		if _, err := repo.GetByRunID(ctx, "later"); !errors.Is(err, deliverypg.ErrTerminalNotFound) {
			t.Fatalf("older result resolved a newer run: %v", err)
		}
		must(repo.Record(ctx, value("z", delivery.TerminalSucceeded, at)))
		must(repo.Record(ctx, value("later", delivery.TerminalFailed, at.Add(time.Hour))))
		must(repo.Record(ctx, value("A", delivery.TerminalFailed, at.Add(3*time.Hour))))
		newer, err := repo.GetByRunID(ctx, "later")
		must(err)
		if newer.State() != delivery.TerminalFailed || !newer.ObservedAt().Equal(at.Add(time.Hour)) {
			t.Fatal("repeated older observation overwrote newer run")
		}
		page, err := repo.ListUnresolvedAfter(ctx, 10, time.Time{}, "")
		must(err)
		if len(page) != 0 {
			t.Fatal("terminal runs still unresolved")
		}
	})

	t.Run("association validation and SQL constraints", func(t *testing.T) {
		if err := repo.Record(ctx, delivery.RunTerminal{}); err == nil {
			t.Fatal("zero record accepted")
		}
		for _, id := range []string{"pending", "missing"} {
			if err := repo.Record(ctx, value(id, delivery.TerminalFailed, at)); !errors.Is(err, deliverypg.ErrRunNotSubmitted) {
				t.Fatalf("unsubmitted: %v", err)
			}
		}
		seed("wrong", at, true)
		wrong, err := delivery.NewRunTerminal("wrong", "other-job", delivery.TerminalFailed, at)
		must(err)
		if err := repo.Record(ctx, wrong); !errors.Is(err, deliverypg.ErrTerminalConflict) {
			t.Fatalf("association mismatch: %v", err)
		}
		for _, tt := range []struct{ sql, code string }{
			{`INSERT INTO run_terminals VALUES ('wrong','other-job','failed',$1)`, "23503"},
			{`INSERT INTO run_terminals VALUES ('pending','job-pending','failed',$1)`, "23503"},
			{`INSERT INTO run_terminals VALUES ('wrong','job-wrong','running',$1)`, "23514"},
			{`INSERT INTO run_terminals VALUES ('wrong','job-wrong','failed',TIMESTAMPTZ '0001-01-01 00:00:00+00')`, "23514"},
			{`INSERT INTO run_terminals VALUES ('wrong','job-wrong','failed',TIMESTAMPTZ 'infinity')`, "23514"},
			{`INSERT INTO run_terminals VALUES ('A','job-A','failed',$1)`, "23505"},
		} {
			var err error
			if strings.Contains(tt.sql, "$1") {
				_, err = pool.Exec(ctx, tt.sql, at)
			} else {
				_, err = pool.Exec(ctx, tt.sql)
			}
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != tt.code {
				t.Fatalf("constraint: got %v want %s", err, tt.code)
			}
		}
		must(repo.Record(ctx, value("wrong", delivery.TerminalFailed, at)))
		for _, sql := range []string{`DELETE FROM outbox WHERE run_id='A'`, `DELETE FROM delivery_runs WHERE id='A'`, `UPDATE outbox SET mercury_job_id='other' WHERE run_id='A'`} {
			_, err := pool.Exec(ctx, sql)
			var pe *pgconn.PgError
			if !errors.As(err, &pe) || pe.Code != "23503" {
				t.Fatalf("restrictive relationship: %v", err)
			}
		}
	})

	t.Run("caller rollback", func(t *testing.T) {
		seed("tx", at, true)
		tx, err := pool.Begin(ctx)
		must(err)
		defer tx.Rollback(context.Background())
		must(deliverypg.NewTerminalRepository(tx).Record(ctx, value("tx", delivery.TerminalSucceeded, at)))
		must(tx.Rollback(ctx))
		if _, err := repo.GetByRunID(ctx, "tx"); !errors.Is(err, deliverypg.ErrTerminalNotFound) {
			t.Fatalf("rollback: %v", err)
		}
		must(repo.Record(ctx, value("tx", delivery.TerminalSucceeded, at)))
	})

	t.Run("competing states preserve one winner", func(t *testing.T) {
		seed("competing", at, true)
		results := make(chan error, 2)
		for _, state := range []delivery.TerminalState{delivery.TerminalFailed, delivery.TerminalSucceeded} {
			v := value("competing", state, at)
			go func() { results <- repo.Record(ctx, v) }()
		}
		a, b := <-results, <-results
		if !((a == nil && errors.Is(b, deliverypg.ErrTerminalConflict)) || (b == nil && errors.Is(a, deliverypg.ErrTerminalConflict))) {
			t.Fatalf("competing states: %v %v", a, b)
		}
		first, err := repo.GetByRunID(ctx, "competing")
		must(err)
		must(repo.Record(ctx, value("competing", first.State(), at.Add(time.Hour))))
		last, err := repo.GetByRunID(ctx, "competing")
		must(err)
		if first != last {
			t.Fatal("concurrent history overwritten")
		}
	})

	t.Run("concurrent reconcilers use atomic repository", func(t *testing.T) {
		seed("race", at, true)
		start, err := delivery.NewAttemptStart("race-unknown", "race", at)
		must(err)
		attempts := deliverypg.NewAttemptRepository(pool)
		must(attempts.AppendStart(ctx, start))
		entered := make(chan struct{}, 2)
		release := make(chan struct{})
		inspect := terminalInspector(func(ctx context.Context, id workerclient.JobID) (workerclient.Job, error) {
			entered <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return workerclient.Job{}, ctx.Err()
			}
			return workerclient.Job{ID: id, TaskType: "webhook.deliver.v1", Payload: json.RawMessage(`{"delivery_id":"delivery","run_id":"race"}`), State: workerclient.StateSucceeded, CompletedAt: &at}, nil
		})
		results := make(chan error, 2)
		for range 2 {
			r, err := reconciliation.New(repo, inspect, reconciliation.Config{BatchSize: 10, PollInterval: time.Second, OperationTimeout: 10 * time.Second}, func() time.Time { return at.Add(time.Hour) }, func(error) {})
			must(err)
			go func() { results <- r.ReconcileBatch(ctx) }()
		}
		for range 2 {
			select {
			case <-entered:
			case <-ctx.Done():
				close(release)
				t.Fatal(ctx.Err())
			}
		}
		close(release)
		for range 2 {
			must(<-results)
		}
		v, err := repo.GetByRunID(ctx, "race")
		must(err)
		if v.State() != delivery.TerminalSucceeded {
			t.Fatal("wrong terminal state")
		}
		history, err := attempts.GetByID(ctx, "race-unknown")
		must(err)
		if history.Outcome != nil {
			t.Fatal("reconciler invented outcome")
		}
	})

	t.Run("migration down and reapply preserves other history", func(t *testing.T) {
		for _, path := range []string{"../../../migrations/000006_run_terminals.down.sql", "../../../migrations/000006_run_terminals.up.sql"} {
			sql, err := os.ReadFile(path)
			must(err)
			_, err = pool.Exec(ctx, string(sql))
			must(err)
		}
		var outcomes, starts int
		must(pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM attempt_outcomes),(SELECT count(*) FROM attempt_starts)`).Scan(&outcomes, &starts))
		if outcomes != 0 || starts != 2 {
			t.Fatalf("attempt history changed: %d %d", starts, outcomes)
		}
		if _, err := repo.GetByRunID(ctx, "A"); !errors.Is(err, deliverypg.ErrTerminalNotFound) {
			t.Fatalf("down retained terminal: %v", err)
		}
	})
}
