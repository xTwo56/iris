package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/event"
)

// ErrHistoryParentNotFound distinguishes a missing parent from an empty page.
var ErrHistoryParentNotFound = errors.New("history parent not found")

// MaxHistoryLimit bounds a page; queries fetch one additional lookahead record.
const MaxHistoryLimit = 200

// HistoryCursor is an exclusive boundary in immutable timestamp/bytewise-ID order.
// The zero value starts a collection; changing observations never changes order.
type HistoryCursor struct {
	At time.Time
	ID string
}

func (c HistoryCursor) validate(limit int) error {
	if limit < 1 || limit > MaxHistoryLimit || c.At.IsZero() != (c.ID == "") || (c.ID != "" && strings.TrimSpace(c.ID) == "") {
		return errors.New("invalid history page")
	}
	if !c.At.IsZero() && (c.At.Year() < 1 || c.At.Year() > 9999 || c.At.Nanosecond()%1000 != 0) {
		return errors.New("invalid history cursor timestamp")
	}
	return nil
}
func (c HistoryCursor) timestamp() any {
	if c.At.IsZero() {
		return nil
	}
	return c.At
}

// RunHistory keeps Mercury's terminal observation separate from HTTP outcomes.
// A nil Terminal means unresolved, not necessarily still executing.
type RunHistory struct {
	Run      delivery.Run
	Terminal *delivery.RunTerminal
}

// HistoryRepository reads bounded pages using a caller-owned pool or transaction.
// Parent checks use one additional query per page, never one query per item.
// It does not contact Mercury, acquire execution locks, or alter history.
type HistoryRepository struct{ db RunQueries }

// NewHistoryRepository borrows existing query resources without taking ownership.
func NewHistoryRepository(db RunQueries) *HistoryRepository {
	return &HistoryRepository{db: db}
}

// GetByID delegates immutable delivery reconstruction to the existing repository.
func (r *HistoryRepository) GetByID(ctx context.Context, id delivery.ID) (delivery.Delivery, error) {
	return New(r.db).GetByID(ctx, id)
}
func (r *HistoryRepository) parent(ctx context.Context, query, id string) error {
	var exists bool
	if err := r.db.QueryRow(ctx, query, id).Scan(&exists); err != nil {
		return fmt.Errorf("history parent lookup: %w", err)
	}
	if !exists {
		return ErrHistoryParentNotFound
	}
	return nil
}

// DeliveryPage checks that the event exists, then reads at most limit+1 rows.
// The extra row lets HTTP advertise a next cursor without counting all history.
func (r *HistoryRepository) DeliveryPage(ctx context.Context, id event.ID, limit int, c HistoryCursor) ([]delivery.Delivery, error) {
	if err := c.validate(limit); err != nil {
		return nil, err
	}
	if err := r.parent(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE id=$1)`, string(id)); err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, `SELECT id,event_id,endpoint_id,created_at FROM deliveries
 WHERE event_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id COLLATE "C")>($2,$3 COLLATE "C"))
 ORDER BY created_at,id COLLATE "C" LIMIT $4`, string(id), c.timestamp(), c.ID, limit+1)
	if err != nil {
		return nil, fmt.Errorf("delivery history: query: %w", err)
	}
	defer rows.Close()
	result := make([]delivery.Delivery, 0)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("delivery history: scan: %w", err)
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("delivery history: iterate: %w", err)
	}
	return result, nil
}

// RunPage joins optional observations in one statement. No observation is a
// valid unresolved entry; only absent delivery parents produce a lookup error.
func (r *HistoryRepository) RunPage(ctx context.Context, id delivery.ID, limit int, c HistoryCursor) ([]RunHistory, error) {
	if err := c.validate(limit); err != nil {
		return nil, err
	}
	if err := r.parent(ctx, `SELECT EXISTS(SELECT 1 FROM deliveries WHERE id=$1)`, string(id)); err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, `SELECT r.id,r.delivery_id,r.created_at,r.trigger,t.mercury_job_id,t.state,t.observed_at
 FROM delivery_runs r LEFT JOIN run_terminals t ON t.run_id=r.id
 WHERE r.delivery_id=$1 AND ($2::timestamptz IS NULL OR (r.created_at,r.id COLLATE "C")>($2,$3 COLLATE "C"))
 ORDER BY r.created_at,r.id COLLATE "C" LIMIT $4`, string(id), c.timestamp(), c.ID, limit+1)
	if err != nil {
		return nil, fmt.Errorf("run history: query: %w", err)
	}
	defer rows.Close()
	result := make([]RunHistory, 0)
	for rows.Next() {
		var runID, deliveryID, trigger string
		var at time.Time
		var job, state *string
		var observed *time.Time
		if err := rows.Scan(&runID, &deliveryID, &at, &trigger, &job, &state, &observed); err != nil {
			return nil, fmt.Errorf("run history: scan: %w", err)
		}
		run, err := delivery.NewRun(delivery.RunID(runID), delivery.ID(deliveryID), at, delivery.Trigger(trigger))
		if err != nil {
			return nil, fmt.Errorf("run history: reconstruct: %w", err)
		}
		item := RunHistory{Run: run}
		if job != nil || state != nil || observed != nil {
			if job == nil || state == nil || observed == nil {
				return nil, errors.New("run history: incomplete observation")
			}
			terminal, err := delivery.NewRunTerminal(run.ID(), *job, delivery.TerminalState(*state), *observed)
			if err != nil {
				return nil, fmt.Errorf("run history: reconstruct terminal: %w", err)
			}
			item.Terminal = &terminal
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("run history: iterate: %w", err)
	}
	return result, nil
}

// AttemptPage retains starts without outcomes. The existing joined decoder
// distinguishes an absent outcome from an observed failure with no HTTP status.
func (r *HistoryRepository) AttemptPage(ctx context.Context, id delivery.RunID, limit int, c HistoryCursor) ([]AttemptHistory, error) {
	if err := c.validate(limit); err != nil {
		return nil, err
	}
	if err := r.parent(ctx, `SELECT EXISTS(SELECT 1 FROM delivery_runs WHERE id=$1)`, string(id)); err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, attemptSelect+`WHERE s.run_id=$1 AND ($2::timestamptz IS NULL OR (s.started_at,s.id COLLATE "C")>($2,$3 COLLATE "C"))
 ORDER BY s.started_at,s.id COLLATE "C" LIMIT $4`, string(id), c.timestamp(), c.ID, limit+1)
	if err != nil {
		return nil, fmt.Errorf("attempt history: query: %w", err)
	}
	defer rows.Close()
	result := make([]AttemptHistory, 0)
	for rows.Next() {
		item, err := scanAttempt(rows)
		if err != nil {
			return nil, fmt.Errorf("attempt history: scan: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("attempt history: iterate: %w", err)
	}
	return result, nil
}
