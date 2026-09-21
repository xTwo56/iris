// Package redelivery creates another execution cycle for an existing obligation.
// It never sends HTTP or changes earlier delivery history.
package redelivery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/xTwo56/iris/internal/delivery"
	deliverypg "github.com/xTwo56/iris/internal/delivery/postgres"
	"github.com/xTwo56/iris/internal/outbox"
	outboxpg "github.com/xTwo56/iris/internal/outbox/postgres"
)

// These errors distinguish invalid requests, missing obligations, key reuse and
// the absence of confirmed terminal observations without exposing SQL details.
var (
	ErrInvalidInput  = errors.New("invalid redelivery input")
	ErrNotFound      = errors.New("delivery not found")
	ErrConflict      = errors.New("redelivery key belongs to another request")
	ErrUnresolvedRun = errors.New("delivery has an unreconciled run")
)

// Beginner borrows the API's pool. The service owns each transaction, but never
// closes the pool or holds a transaction across a network request.
type Beginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// Result identifies the committed execution cycle. Replayed describes this call,
// not a new run or another allocation of Mercury's execution retry budget.
type Result struct {
	DeliveryID delivery.ID
	RunID      delivery.RunID
	Replayed   bool
}

// Service coordinates manual-run creation; execution and retries remain with
// the dispatcher, worker and Mercury after the transaction commits.
type Service struct {
	db    Beginner
	runID func() (delivery.RunID, error)
	now   func() time.Time
}

// New injects metadata generation only for new work; replays never invoke it.
func New(db Beginner, runID func() (delivery.RunID, error), now func() time.Time) *Service {
	return &Service{db: db, runID: runID, now: now}
}

// Redeliver saves one new run and submission intent together. A database-wide key
// in the manual-redelivery namespace compares only the exact delivery ID; there
// are no caller-controlled options. Generated IDs and times are not request input.
func (s *Service) Redeliver(ctx context.Context, key string, id delivery.ID) (result Result, err error) {
	if strings.TrimSpace(key) == "" || strings.TrimSpace(string(id)) == "" {
		return Result{}, ErrInvalidInput
	}
	if s.db == nil || s.runID == nil || s.now == nil {
		return Result{}, errors.New("redeliver: missing dependency")
	}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return Result{}, fmt.Errorf("redeliver: begin: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := tx.Rollback(cleanup); e != nil && !errors.Is(e, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("redeliver: rollback: %w", e))
			result = Result{}
		}
	}()
	// Acquire the request before locking a delivery. Competing inserts wait on the
	// unique key without aborting the transaction. A subsequent READ COMMITTED
	// statement sees the winner's result; rollback instead lets this insert win.
	tag, err := tx.Exec(ctx, `INSERT INTO redelivery_requests (submission_key,requested_delivery_id) VALUES ($1,$2) ON CONFLICT (submission_key) DO NOTHING`, key, string(id))
	if err != nil {
		return Result{}, fmt.Errorf("redeliver: acquire key: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var stored string
		var run *string
		if err := tx.QueryRow(ctx, `SELECT requested_delivery_id,run_id FROM redelivery_requests WHERE submission_key=$1`, key).Scan(&stored, &run); err != nil {
			return Result{}, fmt.Errorf("redeliver: read replay: %w", err)
		}
		if stored != string(id) {
			return Result{}, ErrConflict
		}
		if run == nil {
			return Result{}, errors.New("redeliver: incomplete committed result")
		}
		// Resolve replay before checking unresolved work, including the run this very
		// request created. A lost response must not turn a successful request into 409.
		if err := tx.Commit(ctx); err != nil {
			return Result{}, fmt.Errorf("redeliver replay: commit: %w", err)
		}
		return Result{DeliveryID: id, RunID: delivery.RunID(*run), Replayed: true}, nil
	}
	// Serialize new requests for this delivery. Every manual-run creator must take
	// this lock; after waiting, the next statement observes the previous commit.
	// No routing flags participate: this delivery is already an obligation.
	var locked string
	if err := tx.QueryRow(ctx, `SELECT id FROM deliveries WHERE id=$1 FOR UPDATE`, string(id)).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Result{}, ErrNotFound
		}
		return Result{}, fmt.Errorf("redeliver: lock delivery: %w", err)
	}
	// Inspect every run, including unacknowledged outbox work. Only a persisted
	// terminal observation permits progress; missing attempt outcomes stay unknown.
	var unresolved bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM delivery_runs r WHERE r.delivery_id=$1 AND NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id=r.id))`, string(id)).Scan(&unresolved); err != nil {
		return Result{}, fmt.Errorf("redeliver: inspect run observations: %w", err)
	}
	if unresolved {
		return Result{}, ErrUnresolvedRun
	}
	runID, err := s.runID()
	if err != nil {
		return Result{}, fmt.Errorf("redeliver: generate run ID: %w", err)
	}
	at := s.now()
	run, err := delivery.NewRun(runID, id, at, delivery.TriggerManualRedelivery)
	if err != nil {
		return Result{}, fmt.Errorf("redeliver: construct run: %w", err)
	}
	if err := deliverypg.NewRunRepository(tx).Create(ctx, run); err != nil {
		return Result{}, fmt.Errorf("redeliver: save run: %w", err)
	}
	// Reuse the dispatcher's stable submission namespace. A new run gets its own
	// job; uncertain submission responses keep using this persisted key.
	entry, err := outbox.New(runID, "iris:delivery-run:"+string(runID), at)
	if err != nil {
		return Result{}, fmt.Errorf("redeliver: construct outbox: %w", err)
	}
	if err := outboxpg.New(tx).Create(ctx, entry); err != nil {
		return Result{}, fmt.Errorf("redeliver: save outbox: %w", err)
	}
	tag, err = tx.Exec(ctx, `UPDATE redelivery_requests SET run_id=$2 WHERE submission_key=$1 AND run_id IS NULL`, key, string(runID))
	if err != nil {
		return Result{}, fmt.Errorf("redeliver: save result: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return Result{}, errors.New("redeliver: pending request missing")
	}
	// All three writes commit together. Failure consumes neither the key nor work;
	// uncertain commit responses are recovered by retrying the original key.
	if err := tx.Commit(ctx); err != nil {
		return Result{}, fmt.Errorf("redeliver: commit: %w", err)
	}
	return Result{DeliveryID: id, RunID: runID}, nil
}
