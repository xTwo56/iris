package acceptance_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/event"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
)

// Each invocation creates its own database and applies the real migration.
// The connection database is used only for CREATE/DROP of that generated name.
func TestAcceptanceIntegration(t *testing.T) {
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
	name := "iris_acceptance_test_" + hex.EncodeToString(suffix[:])
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
	at := time.Date(2026, 9, 18, 0, 0, 0, 123456000, time.UTC)
	workAt := at.Add(time.Hour)
	for _, tt := range []struct {
		id, typ         string
		fanout, enabled bool
	}{{"a", "orders.created", true, true}, {"b", "orders.created", true, true}, {"c", "orders.created", false, true}, {"d", "orders.created", true, false}, {"e", "Orders.Created", true, true}, {"f", "orders.*", true, true}} {
		if _, err := pool.Exec(ctx, `INSERT INTO endpoints VALUES ($1,'https://example.com',$2,$3)`, tt.id, at, tt.fanout); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO subscriptions VALUES ($1,$1,$2,$3,$4)`, tt.id, tt.typ, at, tt.enabled); err != nil {
			t.Fatal(err)
		}
	}
	counter := 0
	ids := acceptance.IDs{Delivery: func() (delivery.ID, error) { counter++; return delivery.ID(fmt.Sprintf("d-%d", counter)), nil }, Run: func() (delivery.RunID, error) { return delivery.RunID(fmt.Sprintf("r-%d", counter)), nil }}
	svc := acceptance.New(pool, ids, func() time.Time { return workAt })
	makeEvent := func(id event.ID, typ event.Type) event.Event {
		t.Helper()
		e, err := event.New(id, typ, at, []byte(" \n{\"number\":1.00, \"escaped\":\"\\u0061\"}\t"))
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	counts := func() []int {
		t.Helper()
		result := []int{}
		for _, table := range []string{"events", "deliveries", "delivery_runs", "outbox", "event_submissions"} {
			var n int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
				t.Fatal(err)
			}
			result = append(result, n)
		}
		return result
	}
	t.Run("all eligible destinations and relationships", func(t *testing.T) {
		e := makeEvent("accepted", "orders.created")
		result, err := svc.Accept(ctx, "key-"+string(e.ID()), e)
		if err != nil {
			t.Fatal(err)
		}
		if result.EventID != e.ID() || result.DeliveryCount != 2 {
			t.Fatalf("bad result %+v", result)
		}
		got, err := eventpg.New(pool).GetByID(ctx, e.ID())
		if err != nil {
			t.Fatal(err)
		}
		if got.Type() != e.Type() || !got.CreatedAt().Equal(at) || string(got.Payload()) != string(e.Payload()) {
			t.Fatal("event changed")
		}
		rows, err := pool.Query(ctx, `SELECT d.endpoint_id,d.created_at,r.id,r.created_at,r.trigger,o.submission_key,o.created_at,o.mercury_job_id,o.submitted_at FROM deliveries d JOIN delivery_runs r ON r.delivery_id=d.id JOIN outbox o ON o.run_id=r.id WHERE d.event_id=$1 ORDER BY d.endpoint_id`, string(e.ID()))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			var ep, run, trigger, key string
			var dAt, rAt, oAt time.Time
			var job *string
			var submitted *time.Time
			if err := rows.Scan(&ep, &dAt, &run, &rAt, &trigger, &key, &oAt, &job, &submitted); err != nil {
				t.Fatal(err)
			}
			if ep != []string{"a", "b"}[n] || !dAt.Equal(workAt) || !rAt.Equal(workAt) || !oAt.Equal(workAt) || trigger != "initial" || key != "iris:delivery-run:"+run || job != nil || submitted != nil {
				t.Fatal("incorrect work relationships")
			}
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Fatal(n)
		}
	})
	t.Run("no matches", func(t *testing.T) {
		result, err := svc.Accept(ctx, "key-no-matches", makeEvent("no-matches", "none"))
		if err != nil || result.DeliveryCount != 0 || result.EventID != "no-matches" {
			t.Fatalf("%+v %v", result, err)
		}
		if _, err := eventpg.New(pool).GetByID(ctx, "no-matches"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("duplicate unchanged", func(t *testing.T) {
		before := fmt.Sprint(counts())
		e, err := event.New("accepted", "different", workAt, []byte(`null`))
		if err != nil {
			t.Fatal(err)
		}
		result, err := svc.Accept(ctx, "different-key", e)
		if !errors.Is(err, acceptance.ErrDuplicateEvent) || result.EventID != "" {
			t.Fatalf("%+v %v", result, err)
		}
		if fmt.Sprint(counts()) != before {
			t.Fatal("duplicate wrote data")
		}
		got, err := eventpg.New(pool).GetByID(ctx, "accepted")
		if err != nil {
			t.Fatal(err)
		}
		if got.Type() != "orders.created" || !got.CreatedAt().Equal(at) {
			t.Fatal("duplicate overwrote event")
		}
	})
	t.Run("rollback after partial fanout", func(t *testing.T) {
		before := fmt.Sprint(counts())
		calls := 0
		failing := acceptance.New(pool, acceptance.IDs{Delivery: func() (delivery.ID, error) { calls++; return delivery.ID(fmt.Sprintf("rollback-d-%d", calls)), nil }, Run: func() (delivery.RunID, error) { return "rollback-run", nil }}, func() time.Time { return workAt })
		// First destination has delivery, run and outbox writes; second delivery is
		// inserted before its duplicate run ID fails. All must disappear together.
		result, err := failing.Accept(ctx, "key-rollback", makeEvent("rollback", "orders.created"))
		if !errors.Is(err, deliverypg.ErrDuplicateRunID) || calls != 2 || result.EventID != "" {
			t.Fatalf("%+v %v calls=%d", result, err, calls)
		}
		if fmt.Sprint(counts()) != before {
			t.Fatal("partial writes survived")
		}
		if _, err := eventpg.New(pool).GetByID(ctx, "rollback"); !errors.Is(err, eventpg.ErrNotFound) {
			t.Fatal(err)
		}
	})
	t.Run("commit error returns no acceptance", func(t *testing.T) {
		before := fmt.Sprint(counts())
		failure := errors.New("commit unavailable")
		wrapped := commitFailureDB{pool: pool, failure: failure}
		service := acceptance.New(wrapped, ids, func() time.Time { return workAt })
		result, err := service.Accept(ctx, "key-commit-error", makeEvent("commit-error", "orders.created"))
		if !errors.Is(err, failure) || result.EventID != "" || result.DeliveryCount != 0 {
			t.Fatalf("reported acceptance: %+v %v", result, err)
		}
		if fmt.Sprint(counts()) != before {
			t.Fatal("commit failure cleanup left writes")
		}
	})
	t.Run("invalid input", func(t *testing.T) {
		before := fmt.Sprint(counts())
		if _, err := svc.Accept(ctx, "invalid-key", event.Event{}); err == nil {
			t.Fatal("invalid accepted")
		}
		if fmt.Sprint(counts()) != before {
			t.Fatal("invalid wrote data")
		}
	})
	t.Run("all destinations without list cap", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `INSERT INTO endpoints SELECT 'bulk-'||n,'https://example.com',$1,true FROM generate_series(1,1001) n`, at); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO subscriptions SELECT 'bulk-'||n,'bulk-'||n,'bulk',$1,true FROM generate_series(1,1001) n`, at); err != nil {
			t.Fatal(err)
		}
		result, err := svc.Accept(ctx, "key-bulk-event", makeEvent("bulk-event", "bulk"))
		if err != nil || result.DeliveryCount != 1001 {
			t.Fatalf("truncated fanout: %+v %v", result, err)
		}
	})
}

// Fail commit before transmitting it to exercise cleanup against a real transaction.
type commitFailureDB struct {
	pool    *pgxpool.Pool
	failure error
}

func (d commitFailureDB) BeginTx(ctx context.Context, options pgx.TxOptions) (pgx.Tx, error) {
	tx, err := d.pool.BeginTx(ctx, options)
	if err != nil {
		return nil, err
	}
	return commitFailureTx{Tx: tx, failure: d.failure}, nil
}

type commitFailureTx struct {
	pgx.Tx
	failure error
}

func (t commitFailureTx) Commit(context.Context) error { return t.failure }
