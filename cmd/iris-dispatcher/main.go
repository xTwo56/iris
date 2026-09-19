// Command iris-dispatcher submits durable outbox intent without running webhooks.
// The management API remains a separate, unchanged command.
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xTwo56/iris/internal/application/dispatcher"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/mercury"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
)

type config struct {
	databaseURL, mercuryURL, credential string
	limits                              dispatcher.Config
	requestTimeout                      time.Duration
}

// loadConfig separates submission credentials from management authentication and
// validates limits before opening resources. Errors never echo configured values.
func loadConfig(getenv func(string) string) (config, error) {
	c := config{databaseURL: getenv("IRIS_DATABASE_URL"), mercuryURL: getenv("IRIS_MERCURY_URL"), credential: getenv("IRIS_MERCURY_SUBMISSION_TOKEN"), limits: dispatcher.Config{BatchSize: 100, PollInterval: time.Second, MaxBackoff: 30 * time.Second, OperationTimeout: 20 * time.Second}, requestTimeout: 10 * time.Second}
	if strings.TrimSpace(c.databaseURL) == "" {
		return config{}, errors.New("IRIS_DATABASE_URL is required")
	}
	if raw := getenv("IRIS_DISPATCHER_BATCH_SIZE"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 1000 {
			return config{}, errors.New("IRIS_DISPATCHER_BATCH_SIZE must be 1–1000")
		}
		c.limits.BatchSize = n
	}
	for name, target := range map[string]*time.Duration{"IRIS_DISPATCHER_POLL_INTERVAL": &c.limits.PollInterval, "IRIS_DISPATCHER_MAX_BACKOFF": &c.limits.MaxBackoff, "IRIS_DISPATCHER_OPERATION_TIMEOUT": &c.limits.OperationTimeout, "IRIS_MERCURY_REQUEST_TIMEOUT": &c.requestTimeout} {
		if raw := getenv(name); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil || value <= 0 {
				return config{}, errors.New(name + " must be a positive duration")
			}
			*target = value
		}
	}
	if c.limits.MaxBackoff < c.limits.PollInterval || c.requestTimeout > c.limits.OperationTimeout {
		return config{}, errors.New("backoff must cover polling interval; operation timeout must cover request timeout")
	}
	client, err := mercury.New(c.mercuryURL, c.credential, c.requestTimeout)
	if err != nil {
		return config{}, errors.New("IRIS_MERCURY_URL or IRIS_MERCURY_SUBMISSION_TOKEN is invalid")
	}
	client.Close()
	return c, nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

// run owns the pool and HTTP client. Signals cancel in-flight operations before
// resources close; no database transaction is held across a submission request.
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
	client, err := mercury.New(c.mercuryURL, c.credential, c.requestTimeout)
	if err != nil {
		return errors.New("Mercury submission configuration failed")
	}
	defer client.Close()
	service, err := dispatcher.New(outboxpg.New(pool), deliverypg.NewRunRepository(pool), deliverypg.New(pool), client, c.limits, time.Now, func(err error) { log.Printf("outbox dispatcher: %s", errorClass(err)) })
	if err != nil {
		return errors.New("dispatcher configuration failed")
	}
	log.Print("outbox dispatcher started")
	if err := service.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return errors.New("dispatcher stopped unexpectedly")
	}
	return nil
}

// errorClass surfaces actionable categories without logging remote bodies,
// credentials, keys, URLs, or raw PostgreSQL diagnostics.
func errorClass(err error) string {
	switch {
	case errors.Is(err, dispatcher.ErrAuthentication):
		return "Mercury authentication rejected; check submission credential"
	case errors.Is(err, dispatcher.ErrConfiguration):
		return "Mercury contract/configuration rejected; check task registration and request policy"
	case errors.Is(err, dispatcher.ErrConflict):
		return "Mercury idempotency conflict; preserve the stored key and investigate"
	case errors.Is(err, outboxpg.ErrSubmissionConflict):
		return "outbox acknowledgment job identity conflict; investigate"
	case errors.Is(err, dispatcher.ErrAcknowledgment):
		return "invalid Mercury acknowledgment; entry remains recoverable"
	case errors.Is(err, dispatcher.ErrTransient), errors.Is(err, context.DeadlineExceeded):
		return "transient submission failure; retrying with backoff"
	default:
		return "outbox persistence or relationship failure; entry remains recoverable"
	}
}
