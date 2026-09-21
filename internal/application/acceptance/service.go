package acceptance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/event"
	eventpg "github.com/xTwo56/iris/internal/event/postgres"
	"github.com/xTwo56/iris/internal/outbox"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
	subscriptionpg "github.com/xTwo56/iris/internal/subscription/postgres"
)

var ErrDuplicateEvent = errors.New("event already exists")

type Beginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type IDs struct {
	Delivery func() (delivery.ID, error)
	Run      func() (delivery.RunID, error)
}

type Result struct {
	EventID       event.ID
	DeliveryCount int
	// Replayed describes this invocation, not the stored acceptance result. It
	// lets callers distinguish a committed replay without a second lookup.
	Replayed bool
}

type Service struct {
	db  Beginner
	ids IDs
	now func() time.Time
}

func New(db Beginner, ids IDs, now func() time.Time) *Service {
	return &Service{db: db, ids: ids, now: now}
}

func (s *Service) Accept(ctx context.Context, key string, e event.Event) (result Result, err error) {
	if strings.TrimSpace(key) == "" {
		return Result{}, errors.New("accept event: submission key must not be blank")
	}
	if _, err := event.New(e.ID(), e.Type(), e.CreatedAt(), e.Payload()); err != nil {
		return Result{}, fmt.Errorf("accept event: validate: %w", err)
	}
	if s.db == nil || s.ids.Delivery == nil || s.ids.Run == nil || s.now == nil {
		return Result{}, errors.New("accept event: missing service dependency")
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Result{}, fmt.Errorf("accept event: begin: %w", err)
	}

	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if cleanupErr := tx.Rollback(cleanup); cleanupErr != nil && !errors.Is(cleanupErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("accept event: rollback: %w", cleanupErr))
			result = Result{}
		}
	}()
	// Resolve replays before routing or generating work. A committed key carries
	// the original result even if subscriptions and endpoint flags have changed.
	replay, err := acquireSubmission(ctx, tx, key, e)
	if err != nil {
		return Result{}, fmt.Errorf("accept event: %w", err)
	}
	if replay != nil {
		if err := tx.Commit(ctx); err != nil {
			return Result{}, fmt.Errorf("accept event replay: commit: %w", err)
		}
		replay.Replayed = true
		return *replay, nil
	}
	if err := eventpg.New(tx).Create(ctx, e); err != nil {
		if errors.Is(err, eventpg.ErrDuplicateID) {
			return Result{}, fmt.Errorf("accept event %q: %w", e.ID(), ErrDuplicateEvent)
		}
		return Result{}, fmt.Errorf("accept event: insert: %w", err)
	}
	// Select routing once. This unbounded query observes both flags in one
	// statement; later flag changes do not cancel the selected obligations.
	// The unique endpoint/type constraint gives one destination per matching rule.
	selected, err := subscriptionpg.New(tx).ListEligible(ctx, e.Type())
	if err != nil {
		return Result{}, fmt.Errorf("accept event: select destinations: %w", err)
	}
	deliveries, runs, entries := deliverypg.New(tx), deliverypg.NewRunRepository(tx), outboxpg.New(tx)
	for _, rule := range selected {
		// Give each destination independent work. Its initial run groups subsequent
		// automatic retries without changing the event or delivery identity.
		id, err := s.ids.Delivery()
		if err != nil {
			return Result{}, fmt.Errorf("accept event: generate delivery ID: %w", err)
		}
		runID, err := s.ids.Run()
		if err != nil {
			return Result{}, fmt.Errorf("accept event: generate run ID: %w", err)
		}
		at := s.now()
		d, err := delivery.New(id, e.ID(), rule.EndpointID(), at)
		if err != nil {
			return Result{}, fmt.Errorf("accept event: construct delivery: %w", err)
		}
		if err := deliveries.Create(ctx, d); err != nil {
			return Result{}, fmt.Errorf("accept event: save delivery: %w", err)
		}
		run, err := delivery.NewRun(runID, id, at, delivery.TriggerInitial)
		if err != nil {
			return Result{}, fmt.Errorf("accept event: construct run: %w", err)
		}
		if err := runs.Create(ctx, run); err != nil {
			return Result{}, fmt.Errorf("accept event: save run: %w", err)
		}
		// Persist submission intent alongside the run. The application namespace and
		// run identity produce one stable key, saved once for future dispatcher reuse.
		entry, err := outbox.New(runID, "iris:delivery-run:"+string(runID), at)
		if err != nil {
			return Result{}, fmt.Errorf("accept event: construct outbox: %w", err)
		}
		if err := entries.Create(ctx, entry); err != nil {
			return Result{}, fmt.Errorf("accept event: save outbox: %w", err)
		}
	}
	if err := completeSubmission(ctx, tx, key, Result{EventID: e.ID(), DeliveryCount: len(selected)}); err != nil {
		return Result{}, fmt.Errorf("accept event: %w", err)
	}
	// Report acceptance only after the complete set of writes is committed.
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("accept event: commit: %w", err)
	}
	return Result{EventID: e.ID(), DeliveryCount: len(selected)}, nil
}
