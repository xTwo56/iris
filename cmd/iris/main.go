// Command iris runs the authenticated Iris API. Migrations are applied separately.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/api"
	"github.com/xTwo56/iris/internal/application/acceptance"
	"github.com/xTwo56/iris/internal/application/endpointcreation"
	"github.com/xTwo56/iris/internal/application/redelivery"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/endpointsecret"
	"github.com/xTwo56/iris/internal/subscription"
	subscriptionpg "github.com/xTwo56/iris/internal/subscription/postgres"
)

type config struct {
	token, databaseURL, address string
	encryptionKey               []byte
}

// loadConfig rejects missing authentication before opening a database or listener.
// Configuration errors never contain credentials or connection strings.
func loadConfig(getenv func(string) string) (config, error) {
	c := config{token: getenv("IRIS_MANAGEMENT_TOKEN"), databaseURL: getenv("IRIS_DATABASE_URL"), address: getenv("IRIS_HTTP_ADDR")}
	if strings.TrimSpace(c.token) == "" {
		return config{}, errors.New("IRIS_MANAGEMENT_TOKEN is required")
	}
	if strings.ContainsAny(c.token, " \t\r\n") {
		return config{}, errors.New("IRIS_MANAGEMENT_TOKEN must not contain whitespace")
	}
	if strings.TrimSpace(c.databaseURL) == "" {
		return config{}, errors.New("IRIS_DATABASE_URL is required")
	}
	key, err := endpointsecret.ParseKey(getenv("IRIS_SECRET_ENCRYPTION_KEY"))
	if err != nil {
		return config{}, errors.New("IRIS_SECRET_ENCRYPTION_KEY must be canonical base64 of 32 bytes")
	}
	c.encryptionKey = key
	if c.address == "" {
		c.address = "127.0.0.1:8080"
	}
	return c, nil
}

// newID supplies opaque text IDs without changing storage or adopting a UUID schema.
func newID(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// run owns the pool and server lifecycle. Signals drain requests before closing
// the pool; timeouts bound clients and shutdown. TLS must be provided by deployment.
func run() error {
	c, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(startup, c.databaseURL)
	if err != nil {
		return errors.New("database configuration failed")
	}
	defer pool.Close()
	if err := pool.Ping(startup); err != nil {
		return errors.New("database connection failed")
	}
	cipher, err := endpointsecret.New(c.encryptionKey)
	clear(c.encryptionKey)
	if err != nil {
		return errors.New("secret encryption configuration failed")
	}
	// Acceptance borrows this pool and owns one transaction per submission. Only
	// delivery/run identities are generated here; producer event fields pass through.
	events := acceptance.New(pool, acceptance.IDs{
		Delivery: func() (delivery.ID, error) { id, err := newID("del_"); return delivery.ID(id), err },
		Run:      func() (delivery.RunID, error) { id, err := newID("run_"); return delivery.RunID(id), err },
	}, func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) })
	// Manual redelivery borrows the same pool, but owns its own short transaction.
	redeliveries := redelivery.New(pool, func() (delivery.RunID, error) { id, err := newID("run_"); return delivery.RunID(id), err }, func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) })
	handler, err := api.New(c.token, api.Dependencies{History: deliverypg.NewHistoryRepository(pool), EventAcceptor: events, Redeliverer: redeliveries, EndpointCreator: endpointcreation.New(pool, cipher), Endpoints: endpointpg.New(pool), Subscriptions: subscriptionpg.New(pool), EndpointID: func() (endpoint.ID, error) { id, err := newID("ep_"); return endpoint.ID(id), err }, SubscriptionID: func() (subscription.ID, error) { id, err := newID("sub_"); return subscription.ID(id), err }, Now: time.Now})
	if err != nil {
		return errors.New("API configuration failed")
	}
	server := &http.Server{Addr: c.address, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10, BaseContext: nil}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("HTTP server failed")
		}
		return nil
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return errors.New("HTTP shutdown timed out")
		}
		return nil
	}
}
