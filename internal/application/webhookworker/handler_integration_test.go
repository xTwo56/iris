package webhookworker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/delivery/transport"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/endpointsecret"
	secretpg "github.com/xTwo56/iris/internal/endpointsecret/postgres"
	"github.com/xTwo56/iris/internal/event"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
	"github.com/xTwo56/iris/internal/outbox"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
)

// Use the repository's disposable-database convention. The sender remains a fake;
// production construction and the real transport's SSRF policy are unchanged.
func TestPendingOutboxAndDisabledRoutingIntegration(t *testing.T) {
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
	name := "iris_worker_test_" + hex.EncodeToString(suffix[:])
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
	f := setup(t)
	f.endpoint.FanoutDisable()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(eventpg.New(pool).Create(ctx, f.event))
	must(endpointpg.New(pool).Create(ctx, f.endpoint))
	must(deliverypg.New(pool).Create(ctx, f.delivery))
	must(deliverypg.NewRunRepository(pool).Create(ctx, f.run))
	// Disabled subscription and endpoint affect future fan-out only.
	_, err = pool.Exec(ctx, `INSERT INTO subscriptions (id,endpoint_id,event_type,created_at,enabled) VALUES ('subscription',$1,$2,$3,false)`, string(f.endpoint.ID()), string(f.event.Type()), f.event.CreatedAt())
	must(err)
	entry, err := outbox.New(f.run.ID(), "iris:delivery-run:"+string(f.run.ID()), f.run.CreatedAt())
	must(err)
	box := outboxpg.New(pool)
	must(box.Create(ctx, entry))
	cipher, err := endpointsecret.New(make([]byte, 32))
	must(err)
	sealed, err := cipher.Encrypt(f.endpoint.ID(), make([]byte, 32))
	must(err)
	secrets := secretpg.New(pool)
	must(secrets.Create(ctx, f.endpoint.ID(), sealed))
	attempts := deliverypg.NewAttemptRepository(pool)
	deps := f.deps
	deps.Runs = deliverypg.NewRunRepository(pool)
	deps.Deliveries = deliverypg.New(pool)
	deps.Events = eventpg.New(pool)
	deps.Endpoints = endpointpg.New(pool)
	deps.Attempts = attempts
	deps.Credentials = func(ctx context.Context, id endpoint.ID) ([]byte, error) { return secrets.Load(ctx, id, cipher) }
	deps.Sender = sendFunc(func(ctx context.Context, _ string, b []byte, e event.ID, d delivery.ID, secret []byte) transport.Observation {
		// A separate connection can see the committed start during the network stage.
		observer, err := pgx.ConnectConfig(ctx, config.ConnConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer observer.Close(context.Background())
		var count int
		must(observer.QueryRow(ctx, `SELECT count(*) FROM attempt_starts WHERE run_id=$1`, string(f.run.ID())).Scan(&count))
		if count != 1 || string(b) != string(f.event.Payload()) || e != f.event.ID() || d != f.delivery.ID() || len(secret) != 32 {
			t.Fatal("incorrect durable send boundary")
		}
		pending, err := box.GetByRunID(ctx, f.run.ID())
		must(err)
		if _, _, submitted := pending.Acknowledgment(); submitted {
			t.Fatal("fixture unexpectedly acknowledged")
		}
		return transport.Observation{HTTPStatus: 204, Classification: delivery.ClassificationSucceeded}
	})
	handler, err := New(deps)
	must(err)
	_, err = handler.Execute(ctx, []byte(validPayload))
	must(err)
	history, err := attempts.ListByRunID(ctx, f.run.ID())
	must(err)
	if len(history) != 1 || history[0].Outcome == nil || history[0].Outcome.Classification() != delivery.ClassificationSucceeded {
		t.Fatal("outcome not persisted")
	}
	pending, err := box.GetByRunID(ctx, f.run.ID())
	must(err)
	if _, _, submitted := pending.Acknowledgment(); submitted {
		t.Fatal("worker modified dispatcher acknowledgment")
	}
}
