// Command iris-worker executes deliveries using Mercury's public HTTP worker SDK.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/webhookworker"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/delivery/transport"
	"github.com/xTwo56/iris/internal/endpoint"
	endpointpg "github.com/xTwo56/iris/internal/endpoint/postgres"
	"github.com/xTwo56/iris/internal/endpointsecret"
	secretpg "github.com/xTwo56/iris/internal/endpointsecret/postgres"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
	"github.com/xtwo56/mercury/remoteworker"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

type config struct {
	databaseURL, mercuryURL, token                              string
	key                                                         []byte
	worker                                                      remoteworker.Config
	requestTimeout, sendTimeout, storageTimeout, cleanupTimeout time.Duration
}

// loadConfig validates role-specific settings without printing credentials. The
// worker needs Iris's encryption key and Mercury's worker bearer token, not the
// management or submission credential. No Mercury database URL is accepted.
func loadConfig(getenv func(string) string) (config, error) {
	c := config{databaseURL: getenv("IRIS_DATABASE_URL"), mercuryURL: getenv("IRIS_MERCURY_URL"), token: getenv("IRIS_MERCURY_WORKER_TOKEN"),
		worker:         remoteworker.Config{WorkerID: workerclient.WorkerID(getenv("IRIS_WORKER_ID")), Concurrency: 4, PollInterval: time.Second, HeartbeatInterval: 20 * time.Second, UncertainClaimHold: time.Minute, ShutdownTimeout: 20 * time.Second},
		requestTimeout: 10 * time.Second, sendTimeout: 10 * time.Second, storageTimeout: 5 * time.Second, cleanupTimeout: 2 * time.Second}
	if strings.TrimSpace(c.databaseURL) == "" || strings.TrimSpace(string(c.worker.WorkerID)) == "" {
		return config{}, errors.New("IRIS_DATABASE_URL and IRIS_WORKER_ID are required")
	}
	u, err := url.Parse(c.mercuryURL)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || strings.Contains(c.mercuryURL, "#") || u.Opaque != "" || (u.Path != "" && u.Path != "/") {
		return config{}, errors.New("invalid IRIS_MERCURY_URL origin")
	}
	local := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "::1"
	if u.Scheme != "https" && !(u.Scheme == "http" && local) {
		return config{}, errors.New("IRIS_MERCURY_URL requires HTTPS except on loopback")
	}
	if c.token == "" {
		return config{}, errors.New("IRIS_MERCURY_WORKER_TOKEN is required")
	}
	for _, b := range []byte(c.token) {
		if b < 0x21 || b > 0x7e {
			return config{}, errors.New("IRIS_MERCURY_WORKER_TOKEN must be printable without whitespace")
		}
	}
	if raw := getenv("IRIS_WORKER_CONCURRENCY"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			return config{}, errors.New("IRIS_WORKER_CONCURRENCY must be 1–1000")
		}
		c.worker.Concurrency = n
	}
	for name, target := range map[string]*time.Duration{
		"IRIS_WORKER_POLL_INTERVAL": &c.worker.PollInterval, "IRIS_WORKER_HEARTBEAT_INTERVAL": &c.worker.HeartbeatInterval, "IRIS_WORKER_SHUTDOWN_TIMEOUT": &c.worker.ShutdownTimeout,
		"IRIS_MERCURY_REQUEST_TIMEOUT": &c.requestTimeout, "IRIS_WEBHOOK_TIMEOUT": &c.sendTimeout, "IRIS_WORKER_STORAGE_TIMEOUT": &c.storageTimeout, "IRIS_WORKER_CLEANUP_TIMEOUT": &c.cleanupTimeout} {
		if raw := getenv(name); raw != "" {
			v, err := time.ParseDuration(raw)
			if err != nil || v <= 0 {
				return config{}, errors.New(name + " must be a positive duration")
			}
			*target = v
		}
	}
	if c.worker.HeartbeatInterval >= remoteworker.V1LeaseDuration/2 || c.requestTimeout >= remoteworker.V1LeaseDuration-c.worker.HeartbeatInterval {
		return config{}, errors.New("worker heartbeat/request bounds must leave lease renewal margin")
	}
	if c.worker.ShutdownTimeout < c.cleanupTimeout || c.worker.ShutdownTimeout-c.cleanupTimeout < c.storageTimeout {
		return config{}, errors.New("shutdown timeout must cover storage and cleanup bounds")
	}
	c.key, err = endpointsecret.ParseKey(getenv("IRIS_SECRET_ENCRYPTION_KEY"))
	if err != nil {
		return config{}, errors.New("IRIS_SECRET_ENCRYPTION_KEY must be canonical base64 of 32 bytes")
	}
	return c, nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// run owns Iris's pool and the SDK's HTTP client. The SDK alone polls, filters
// tasks, starts executions, renews leases and reports outcomes. Shutdown cancels
// execution through that runtime before closing connections. No migrations run.
func run() error {
	c, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	defer clear(c.key)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(startup, c.databaseURL)
	if err != nil {
		return errors.New("Iris database configuration failed")
	}
	defer pool.Close()
	if err := pool.Ping(startup); err != nil {
		return errors.New("Iris database connection failed")
	}
	cipher, err := endpointsecret.New(c.key)
	clear(c.key)
	if err != nil {
		return errors.New("endpoint secret encryption configuration failed")
	}
	sender, err := transport.New(transport.Config{Timeout: c.sendTimeout, MaxResponseBytes: 4096})
	if err != nil {
		return errors.New("webhook transport configuration failed")
	}
	signed, err := transport.NewSigned(sender, time.Now)
	if err != nil {
		return errors.New("webhook signing configuration failed")
	}
	secrets := secretpg.New(pool)
	handler, err := webhookworker.New(webhookworker.Dependencies{
		Runs: deliverypg.NewRunRepository(pool), Deliveries: deliverypg.New(pool), Events: eventpg.New(pool), Endpoints: endpointpg.New(pool), Attempts: deliverypg.NewAttemptRepository(pool), Sender: signed,
		Credentials: func(ctx context.Context, id endpoint.ID) ([]byte, error) { return secrets.Load(ctx, id, cipher) }, NewAttemptID: newAttemptID, Now: time.Now, StorageTimeout: c.storageTimeout, CleanupTimeout: c.cleanupTimeout})
	if err != nil {
		return errors.New("webhook handler configuration failed")
	}
	registry := remoteworker.NewRegistry()
	if err := registry.Register(workerclient.TaskType(webhookworker.TaskType), handler); err != nil {
		return errors.New("webhook handler registration failed")
	}
	httpTransport := http.DefaultTransport.(*http.Transport).Clone()
	httpTransport.Proxy = nil
	httpTransport.MaxResponseHeaderBytes = 32 << 10
	defer httpTransport.CloseIdleConnections()
	lifecycle, err := workerclient.New(workerclient.Config{ServerURL: c.mercuryURL, BearerToken: c.token, HTTPClient: &http.Client{Transport: httpTransport, Timeout: c.requestTimeout}})
	if err != nil {
		return errors.New("Mercury worker client configuration failed")
	}
	runtime, err := remoteworker.New(lifecycle, registry, c.worker, slog.Default())
	if err != nil {
		return errors.New("Mercury worker runtime configuration failed")
	}
	log.Print("Iris webhook worker started")
	if err := runtime.Run(ctx); err != nil {
		return errors.New("Mercury worker runtime failed or shutdown timed out")
	}
	return nil
}

// newAttemptID gives each execution its own history identity. Event, delivery and
// run identities are read from stored records and remain unchanged across retries.
func newAttemptID() (delivery.AttemptID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return delivery.AttemptID("att_" + hex.EncodeToString(b[:])), nil
}
