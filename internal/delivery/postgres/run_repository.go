package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/delivery"
)

var ErrDuplicateRunID = errors.New("run ID already exists")

// indicates the delivery already has an initial run.
var ErrDuplicateInitialRun = errors.New("delivery initial run already exists")

var ErrDeliveryNotFound = errors.New("run delivery not found")

var ErrRunNotFound = errors.New("run not found")

// caller-owned *pgxpool.Pool and pgx.Tx satisfy it
type RunQueries interface {
	Queries
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type RunRepository struct{ db RunQueries }

func NewRunRepository(db RunQueries) *RunRepository { return &RunRepository{db: db} }

func (r *RunRepository) Create(ctx context.Context, run delivery.Run) error {
	if _, err := delivery.NewRun(run.ID(), run.DeliveryID(), run.CreatedAt(), run.Trigger()); err != nil {
		return fmt.Errorf("create run: validate: %w", err)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO delivery_runs (id,delivery_id,created_at,trigger) VALUES ($1,$2,$3,$4)`, string(run.ID()), string(run.DeliveryID()), run.CreatedAt(), string(run.Trigger()))
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch {
			case pgErr.Code == "23505" && pgErr.ConstraintName == "delivery_runs_pkey":
				err = ErrDuplicateRunID
			case pgErr.Code == "23505" && pgErr.ConstraintName == "delivery_runs_one_initial_per_delivery":
				err = ErrDuplicateInitialRun
			case pgErr.Code == "23503" && pgErr.ConstraintName == "delivery_runs_delivery_fk":
				err = ErrDeliveryNotFound
			}
		}
		return fmt.Errorf("create run %q: %w", run.ID(), err)
	}
	return nil
}

func scanRun(row interface{ Scan(...any) error }) (delivery.Run, error) {
	var id, deliveryID, trigger string
	var at time.Time
	if err := row.Scan(&id, &deliveryID, &at, &trigger); err != nil {
		return delivery.Run{}, err
	}
	run, err := delivery.NewRun(delivery.RunID(id), delivery.ID(deliveryID), at, delivery.Trigger(trigger))
	if err != nil {
		return delivery.Run{}, fmt.Errorf("reconstruct: %w", err)
	}
	return run, nil
}

func (r *RunRepository) GetByID(ctx context.Context, id delivery.RunID) (delivery.Run, error) {
	run, err := scanRun(r.db.QueryRow(ctx, `SELECT id,delivery_id,created_at,trigger FROM delivery_runs WHERE id=$1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrRunNotFound
	}
	if err != nil {
		return delivery.Run{}, fmt.Errorf("get run %q: %w", id, err)
	}
	return run, nil
}

func (r *RunRepository) ListByDeliveryID(ctx context.Context, id delivery.ID) ([]delivery.Run, error) {
	rows, err := r.db.Query(ctx, `SELECT id,delivery_id,created_at,trigger FROM delivery_runs WHERE delivery_id=$1 ORDER BY created_at, id COLLATE "C"`, string(id))
	if err != nil {
		return nil, fmt.Errorf("list runs for delivery %q: %w", id, err)
	}
	defer rows.Close()
	result := make([]delivery.Run, 0)
	for rows.Next() {
		run, err := scanRun(rows)
		if err != nil {
			return nil, fmt.Errorf("list runs for delivery %q: %w", id, err)
		}
		result = append(result, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list runs for delivery %q: iterate: %w", id, err)
	}
	return result, nil
}
