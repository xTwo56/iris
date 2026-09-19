package endpointcreation_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"github.com/xTwo56/iris/internal/application/endpointcreation"
	"github.com/xTwo56/iris/internal/endpointsecret"
	secretpg "github.com/xTwo56/iris/internal/endpointsecret/postgres"
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
func TestCreationIntegration(t *testing.T) {
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
	name := "iris_secret_test_" + hex.EncodeToString(suffix[:])
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
	migration, err = os.ReadFile("../../../migrations/000005_endpoint_secrets.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, string(migration)); err != nil {
		t.Fatal(err)
	}
	cipher, err := endpointsecret.New(bytes.Repeat([]byte{3}, 32))
	if err != nil {
		t.Fatal(err)
	}
	service := endpointcreation.New(pool, cipher)
	repo := secretpg.New(pool)
	at := time.Date(2026, 9, 19, 0, 0, 0, 123456000, time.UTC)
	makeEndpoint := func(id endpoint.ID) endpoint.Endpoint {
		t.Helper()
		e, err := endpoint.New(id, "https://example.com", at)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	t.Run("encrypted creation and retrieval", func(t *testing.T) {
		encoded, err := service.Create(ctx, makeEndpoint("ep"))
		if err != nil {
			t.Fatal(err)
		}
		plain, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(plain) != 32 {
			t.Fatal("bad plaintext response")
		}
		sealed, err := repo.GetByEndpointID(ctx, "ep")
		if err != nil {
			t.Fatal(err)
		}
		if sealed.Format != 1 || len(sealed.Nonce) != 12 || len(sealed.Ciphertext) != 48 || bytes.Contains(sealed.Ciphertext, plain) {
			t.Fatal("bad encrypted storage")
		}
		loaded, err := repo.Load(ctx, "ep", cipher)
		if err != nil || !bytes.Equal(loaded, plain) {
			t.Fatal("decrypt mismatch")
		}
		wrong, _ := endpointsecret.New(bytes.Repeat([]byte{5}, 32))
		if _, err := repo.Load(ctx, "ep", wrong); err == nil {
			t.Fatal("wrong key decrypted")
		}
		if _, err := cipher.Decrypt("another", sealed); err == nil {
			t.Fatal("wrong endpoint decrypted")
		}
		if _, err := service.Create(ctx, makeEndpoint("ep")); !errors.Is(err, endpointpg.ErrDuplicateID) {
			t.Fatal(err)
		}
		again, err := repo.Load(ctx, "ep", cipher)
		if err != nil || !bytes.Equal(again, plain) {
			t.Fatal("duplicate replaced secret")
		}
		second, err := service.Create(ctx, makeEndpoint("ep2"))
		if err != nil || second == encoded {
			t.Fatal("per-endpoint secret not unique")
		}
	})
	t.Run("legacy endpoint remains without secret", func(t *testing.T) {
		if err := endpointpg.New(pool).Create(ctx, makeEndpoint("legacy")); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Load(ctx, "legacy", cipher); !errors.Is(err, secretpg.ErrNotFound) {
			t.Fatal("missing credential not explicit")
		}
		if _, err := endpointpg.New(pool).GetByID(ctx, "legacy"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("secret insert failure rolls endpoint back", func(t *testing.T) {
		// Test-only constraint forces the second write to fail after endpoint INSERT.
		if _, err := pool.Exec(ctx, `ALTER TABLE endpoint_secrets ADD CONSTRAINT test_failure CHECK (endpoint_id <> 'fail')`); err != nil {
			t.Fatal(err)
		}
		secret, err := service.Create(ctx, makeEndpoint("fail"))
		if err == nil || secret != "" {
			t.Fatal("failed create exposed secret")
		}
		if _, err := endpointpg.New(pool).GetByID(ctx, "fail"); !errors.Is(err, endpointpg.ErrNotFound) {
			t.Fatal("orphan endpoint survived")
		}
		if _, err := repo.GetByEndpointID(ctx, "fail"); !errors.Is(err, secretpg.ErrNotFound) {
			t.Fatal("secret survived rollback")
		}
		if _, err := pool.Exec(ctx, `ALTER TABLE endpoint_secrets DROP CONSTRAINT test_failure`); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("caller transaction and constraints", func(t *testing.T) {
		sealed, err := cipher.Encrypt("tx", bytes.Repeat([]byte{9}, 32))
		if err != nil {
			t.Fatal(err)
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(context.Background())
		if err := endpointpg.New(tx).Create(ctx, makeEndpoint("tx")); err != nil {
			t.Fatal(err)
		}
		if err := secretpg.New(tx).Create(ctx, "tx", sealed); err != nil {
			t.Fatal(err)
		}
		if _, err := secretpg.New(tx).Load(ctx, "tx", cipher); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.GetByEndpointID(ctx, "tx"); !errors.Is(err, secretpg.ErrNotFound) {
			t.Fatal(err)
		}
		if err := repo.Create(ctx, "missing", sealed); !errors.Is(err, secretpg.ErrEndpointNotFound) {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM endpoints WHERE id='ep'`); err == nil {
			t.Fatal("deletion not restricted")
		}
		if _, err := pool.Exec(ctx, `UPDATE endpoint_secrets SET nonce='x' WHERE endpoint_id='ep'`); err == nil {
			t.Fatal("bad nonce accepted")
		}
		if _, err := pool.Exec(ctx, `UPDATE endpoint_secrets SET encryption_format=2 WHERE endpoint_id='ep'`); err == nil {
			t.Fatal("unknown format accepted")
		}
	})
	t.Run("down and reapply preserve endpoints", func(t *testing.T) {
		down, err := os.ReadFile("../../../migrations/000005_endpoint_secrets.down.sql")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(down)); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(migration)); err != nil {
			t.Fatal(err)
		}
		if _, err := endpointpg.New(pool).GetByID(ctx, "ep"); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Load(ctx, "ep", cipher); !errors.Is(err, secretpg.ErrNotFound) {
			t.Fatal("unexpected backfill")
		}
	})
}
