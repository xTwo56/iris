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
)

// ErrDuplicateAttemptID indicates a start already exists with this identity.
var ErrDuplicateAttemptID = errors.New("attempt ID already exists")

// ErrDuplicateOutcome indicates an observation already exists for this attempt.
var ErrDuplicateOutcome = errors.New("attempt outcome already exists")

// ErrAttemptStartNotFound indicates an outcome references a missing start.
var ErrAttemptStartNotFound = errors.New("attempt start not found")

// ErrAttemptNotFound indicates no start matched a history lookup.
var ErrAttemptNotFound = errors.New("attempt not found")

// AttemptHistory combines a start with its optional observation. Outcome is nil
// when the result is unknown, not when delivery is confirmed to have failed.
// The values are reconstructed domain records, independent of database buffers.
type AttemptHistory struct {
	Start   delivery.AttemptStart
	Outcome *delivery.AttemptOutcome
}

// AttemptRepository appends and reads history using a caller-owned pool or
// transaction. It never commits, rolls back or closes the caller's resources.
// Future workers must durably save the start before sending HTTP and append the
// outcome afterward; never hold one database transaction across the network call.
// Append-only use assumes starts remain immutable; this API exposes no mutations.
type AttemptRepository struct{ db RunQueries }

// NewAttemptRepository reuses the narrow delivery query surface, including Query
// for lists, without opening connections or applying migrations.
func NewAttemptRepository(db RunQueries) *AttemptRepository { return &AttemptRepository{db: db} }

// AppendStart validates before insertion. SQLSTATE and named constraints map
// duplicate identities and missing runs; other errors preserve their cause.
func (r *AttemptRepository) AppendStart(ctx context.Context, s delivery.AttemptStart) error {
	if _, err := delivery.NewAttemptStart(s.ID(), s.RunID(), s.StartedAt()); err != nil {
		return fmt.Errorf("append attempt start: validate: %w", err)
	}
	_, err := r.db.Exec(ctx, `INSERT INTO attempt_starts (id,run_id,started_at) VALUES ($1,$2,$3)`, string(s.ID()), string(s.RunID()), s.StartedAt())
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			if pe.Code == "23505" && pe.ConstraintName == "attempt_starts_pkey" {
				err = ErrDuplicateAttemptID
			}
			if pe.Code == "23503" && pe.ConstraintName == "attempt_starts_run_fk" {
				err = ErrRunNotFound
			}
		}
		return fmt.Errorf("append attempt start %q: %w", s.ID(), err)
	}
	return nil
}

// AppendOutcome loads the actual persisted start and reconstructs the outcome
// against it before inserting. A caller's earlier start time cannot bypass this
// validation. Nullable status is passed separately from its value, preserving
// absence. The primary key enforces one observation even for concurrent inserts.
func (r *AttemptRepository) AppendOutcome(ctx context.Context, o delivery.AttemptOutcome) error {
	if strings.TrimSpace(string(o.AttemptID())) == "" || o.FinishedAt().IsZero() {
		return errors.New("append attempt outcome: invalid identity or zero finish timestamp")
	}
	var id, runID string
	var at time.Time
	err := r.db.QueryRow(ctx, `SELECT id,run_id,started_at FROM attempt_starts WHERE id=$1`, string(o.AttemptID())).Scan(&id, &runID, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrAttemptStartNotFound
	}
	if err != nil {
		return fmt.Errorf("append attempt outcome %q: load start: %w", o.AttemptID(), err)
	}
	start, err := delivery.NewAttemptStart(delivery.AttemptID(id), delivery.RunID(runID), at)
	if err != nil {
		return fmt.Errorf("append attempt outcome %q: reconstruct start: %w", o.AttemptID(), err)
	}
	status, present := o.HTTPStatus()
	var statusPtr *int
	if present {
		statusPtr = &status
	}
	if _, err := delivery.NewAttemptOutcome(start, o.FinishedAt(), statusPtr, o.Classification()); err != nil {
		return fmt.Errorf("append attempt outcome %q: validate: %w", o.AttemptID(), err)
	}
	_, err = r.db.Exec(ctx, `INSERT INTO attempt_outcomes (attempt_id,finished_at,http_status,classification) VALUES ($1,$2,$3,$4)`, string(o.AttemptID()), o.FinishedAt(), statusPtr, string(o.Classification()))
	if err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) {
			if pe.Code == "23505" && pe.ConstraintName == "attempt_outcomes_pkey" {
				err = ErrDuplicateOutcome
			}
			if pe.Code == "23503" && pe.ConstraintName == "attempt_outcomes_start_fk" {
				err = ErrAttemptStartNotFound
			}
		}
		return fmt.Errorf("append attempt outcome %q: %w", o.AttemptID(), err)
	}
	return nil
}

const attemptSelect = `SELECT s.id,s.run_id,s.started_at,o.attempt_id,o.finished_at,o.http_status,o.classification
 FROM attempt_starts s LEFT JOIN attempt_outcomes o ON o.attempt_id=s.id `

// scanAttempt validates both stored records. The joined outcome ID, not status,
// determines presence: an outcome with NULL status is still a complete record.
func scanAttempt(row interface{ Scan(...any) error }) (AttemptHistory, error) {
	var id, runID string
	var at time.Time
	var outcomeID, classification *string
	var finishedAt *time.Time
	var status *int
	if err := row.Scan(&id, &runID, &at, &outcomeID, &finishedAt, &status, &classification); err != nil {
		return AttemptHistory{}, err
	}
	start, err := delivery.NewAttemptStart(delivery.AttemptID(id), delivery.RunID(runID), at)
	if err != nil {
		return AttemptHistory{}, fmt.Errorf("reconstruct start: %w", err)
	}
	result := AttemptHistory{Start: start}
	if outcomeID != nil {
		if *outcomeID != id || finishedAt == nil || classification == nil {
			return AttemptHistory{}, errors.New("reconstruct outcome: inconsistent stored fields")
		}
		outcome, err := delivery.NewAttemptOutcome(start, *finishedAt, status, delivery.Classification(*classification))
		if err != nil {
			return AttemptHistory{}, fmt.Errorf("reconstruct outcome: %w", err)
		}
		result.Outcome = &outcome
	}
	return result, nil
}

// GetByID uses one left join to observe a start and optional outcome together.
// Missing outcomes are valid history; only a missing start maps to ErrAttemptNotFound.
func (r *AttemptRepository) GetByID(ctx context.Context, id delivery.AttemptID) (AttemptHistory, error) {
	h, err := scanAttempt(r.db.QueryRow(ctx, attemptSelect+`WHERE s.id=$1`, string(id)))
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrAttemptNotFound
	}
	if err != nil {
		return AttemptHistory{}, fmt.Errorf("get attempt %q: %w", id, err)
	}
	return h, nil
}

// ListByRunID includes unknown outcomes and orders by start time then bytewise
// attempt ID. Rows close on every path, and iteration errors discard partial
// results. No matching starts returns an empty slice, including for absent runs.
func (r *AttemptRepository) ListByRunID(ctx context.Context, id delivery.RunID) ([]AttemptHistory, error) {
	rows, err := r.db.Query(ctx, attemptSelect+`WHERE s.run_id=$1 ORDER BY s.started_at,s.id COLLATE "C"`, string(id))
	if err != nil {
		return nil, fmt.Errorf("list attempts for run %q: %w", id, err)
	}
	defer rows.Close()
	result := make([]AttemptHistory, 0)
	for rows.Next() {
		h, err := scanAttempt(rows)
		if err != nil {
			return nil, fmt.Errorf("list attempts for run %q: %w", id, err)
		}
		result = append(result, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list attempts for run %q: iterate: %w", id, err)
	}
	return result, nil
}
