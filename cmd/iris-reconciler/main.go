// Command iris-reconciler records terminal Mercury jobs in Iris, without executing work.
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/reconciliation"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	workerclient "github.com/xtwo56/mercury/remoteworker/client"
)

type config struct {
	databaseURL, mercuryURL, token string
	limits                         reconciliation.Config
	requestTimeout                 time.Duration
}

// loadConfig requires only Iris storage and Mercury worker-inspection access.
// This role needs no signing key, producer token, worker identity or Mercury DB.
func loadConfig(getenv func(string) string) (config, error) {
	c := config{databaseURL: getenv("IRIS_DATABASE_URL"), mercuryURL: getenv("IRIS_MERCURY_URL"), token: getenv("IRIS_MERCURY_WORKER_TOKEN"),
		limits: reconciliation.Config{BatchSize: 100, PollInterval: 5 * time.Second, OperationTimeout: 20 * time.Second}, requestTimeout: 10 * time.Second}
	if strings.TrimSpace(c.databaseURL) == "" {
		return config{}, errors.New("IRIS_DATABASE_URL is required")
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
	if raw := getenv("IRIS_RECONCILER_BATCH_SIZE"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > deliverypg.MaxReconciliationBatch {
			return config{}, errors.New("IRIS_RECONCILER_BATCH_SIZE must be 1–1000")
		}
		c.limits.BatchSize = n
	}
	for name, target := range map[string]*time.Duration{
		"IRIS_RECONCILER_POLL_INTERVAL":     &c.limits.PollInterval,
		"IRIS_RECONCILER_OPERATION_TIMEOUT": &c.limits.OperationTimeout,
		"IRIS_MERCURY_REQUEST_TIMEOUT":      &c.requestTimeout,
	} {
		if raw := getenv(name); raw != "" {
			v, err := time.ParseDuration(raw)
			if err != nil || v <= 0 {
				return config{}, errors.New(name + " must be a positive duration")
			}
			*target = v
		}
	}
	if c.requestTimeout > c.limits.OperationTimeout {
		return config{}, errors.New("operation timeout must cover Mercury request timeout")
	}
	return c, nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// run owns both resources and passes only a pool-backed store to reconciliation.
// Signals cancel inspection, storage and polling before resources close. This
// command neither applies migrations nor starts an SDK execution runtime.
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
		return errors.New("Iris database configuration failed")
	}
	defer pool.Close()
	if err := pool.Ping(startup); err != nil {
		return errors.New("Iris database connection failed")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.MaxResponseHeaderBytes = 32 << 10
	defer transport.CloseIdleConnections()
	client, err := workerclient.New(workerclient.Config{ServerURL: c.mercuryURL, BearerToken: c.token,
		HTTPClient: &http.Client{Transport: transport, Timeout: c.requestTimeout}, MaxResponseBytes: 8 << 20})
	if err != nil {
		return errors.New("Mercury inspection configuration failed")
	}
	service, err := reconciliation.New(deliverypg.NewTerminalRepository(pool), client, c.limits, time.Now,
		func(err error) { log.Printf("run reconciler: %s", reconciliation.ErrorClass(err)) })
	if err != nil {
		return errors.New("reconciler configuration failed")
	}
	log.Print("Iris run reconciler started")
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return errors.New("run reconciler stopped unexpectedly")
	}
	return nil
}
