package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/xTwo56/iris/internal/delivery"
	"github.com/xTwo56/iris/internal/outbox"
)

var (
	ErrDuplicateRunID         = errors.New("outbox run already exists")
	ErrDuplicateSubmissionKey = errors.New("outbox submission key already exists")
	ErrRunNotFound            = errors.New("outbox referenced run not found")
	ErrNotFound               = errors.New("outbox entry not found")
	ErrSubmissionConflict     = errors.New("outbox acknowledged job identity conflicts")
)

const MaxPendingLimit = 1000

type Queries interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

type Repository struct{ db Queries }

func New(db Queries) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, e outbox.Entry) error {
	if _, err := outbox.New(e.RunID(), e.SubmissionKey(), e.CreatedAt()); err != nil {
		return fmt.Errorf("create outbox: validate: %w", err)
	}
	if _, _, submitted := e.Acknowledgment(); submitted {
		return errors.New("create outbox: entry must be pending")
	}
	_, err := r.db.Exec(ctx, `INSERT INTO outbox (run_id,submission_key,created_at) VALUES ($1,$2,$3)`, string(e.RunID()), e.SubmissionKey(), e.CreatedAt())
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			switch {
			case pe.Code == "23505" && pe.ConstraintName == "outbox_pkey":
				err = ErrDuplicateRunID
			case pe.Code == "23505" && pe.ConstraintName == "outbox_submission_key_key":
				err = ErrDuplicateSubmissionKey
			case pe.Code == "23503" && pe.ConstraintName == "outbox_run_fk":
				err = ErrRunNotFound
			}
		}
		return fmt.Errorf("create outbox %q: %w", e.RunID(), err)
	}
	return nil
}

func scanEntry(row interface{ Scan(...any) error }) (outbox.Entry, error) {
	var run, key string
	var at time.Time
	var job *string
	var submitted *time.Time
	if err := row.Scan(&run, &key, &at, &job, &submitted); err != nil {
		return outbox.Entry{}, err
	}
	e, err := outbox.New(delivery.RunID(run), key, at)
	if err != nil {
		return outbox.Entry{}, fmt.Errorf("reconstruct: %w", err)
	}
	if (job == nil) != (submitted == nil) {
		return outbox.Entry{}, errors.New("reconstruct: incomplete acknowledgment")
	}
	if job != nil {
		e, err = e.WithAcknowledgment(*job, *submitted)
		if err != nil {
			return outbox.Entry{}, fmt.Errorf("reconstruct acknowledgment: %w", err)
		}
	}
	return e, nil
}

func (r *Repository) GetByRunID(ctx context.Context, id delivery.RunID) (outbox.Entry, error) {
	e, err := scanEntry(r.db.QueryRow(ctx, `SELECT run_id,submission_key,created_at,mercury_job_id,submitted_at FROM outbox WHERE run_id=$1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	if err != nil {
		return outbox.Entry{}, fmt.Errorf("get outbox %q: %w", id, err)
	}
	return e, nil
}

func (r *Repository) ListPending(ctx context.Context, limit int) ([]outbox.Entry, error) {
	if limit <= 0 || limit > MaxPendingLimit {
		return nil, fmt.Errorf("list pending outbox: limit must be 1–%d", MaxPendingLimit)
	}
	rows, err := r.db.Query(ctx, `SELECT run_id,submission_key,created_at,mercury_job_id,submitted_at FROM outbox WHERE mercury_job_id IS NULL ORDER BY created_at,run_id COLLATE "C" LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending outbox: %w", err)
	}
	defer rows.Close()
	result := make([]outbox.Entry, 0)
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("list pending outbox: %w", err)
		}
		result = append(result, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pending outbox: iterate: %w", err)
	}
	return result, nil
}

func (r *Repository) MarkSubmitted(ctx context.Context, id delivery.RunID, job string, at time.Time) error {
	if strings.TrimSpace(string(id)) == "" {
		return errors.New("mark outbox submitted: blank run ID")
	}
	if err := outbox.ValidateAcknowledgment(job, at); err != nil {
		return fmt.Errorf("mark outbox submitted: validate: %w", err)
	}
	var recorded string
	err := r.db.QueryRow(ctx, `UPDATE outbox SET
 mercury_job_id=CASE WHEN mercury_job_id IS NULL THEN $2 ELSE mercury_job_id END,
 submitted_at=CASE WHEN mercury_job_id IS NULL THEN $3 ELSE submitted_at END
 WHERE run_id=$1 RETURNING mercury_job_id`, string(id), job, at).Scan(&recorded)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("mark outbox %q submitted: %w", id, err)
	}
	if recorded != job {
		return fmt.Errorf("mark outbox %q submitted: %w", id, ErrSubmissionConflict)
	}
	return nil
}
