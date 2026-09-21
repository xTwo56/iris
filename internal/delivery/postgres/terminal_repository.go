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

var (
	ErrTerminalConflict = errors.New("run terminal job or state conflicts")
	ErrRunNotSubmitted  = errors.New("run has no acknowledged outbox entry")
	ErrTerminalNotFound = errors.New("run terminal observation not found")
)

const MaxReconciliationBatch = 1000

// TerminalRepository records confirmed results without changing runs or attempt
// history. A caller owns the pool/transaction; methods never commit or roll back.
// The network-facing reconciler must use a pool, never hold a transaction open.
type TerminalRepository struct{ db RunQueries }

func NewTerminalRepository(db RunQueries) *TerminalRepository { return &TerminalRepository{db: db} }

// Record preserves the first observation. INSERT checks the acknowledged job in
// the same statement; the composite foreign key keeps that association valid.
// ON CONFLICT does not update history or abort a caller-owned transaction. A
// following READ COMMITTED lookup sees a concurrent winner and compares identity
// and state, deliberately ignoring a later observer's timestamp.
func (r *TerminalRepository) Record(ctx context.Context, value delivery.RunTerminal) error {
	if _, err := delivery.NewRunTerminal(value.RunID(), value.MercuryJobID(), value.State(), value.ObservedAt()); err != nil {
		return fmt.Errorf("record run terminal: validate: %w", err)
	}
	var inserted string
	err := r.db.QueryRow(ctx, `INSERT INTO run_terminals (run_id,mercury_job_id,state,observed_at)
 SELECT run_id,mercury_job_id,$3,$4 FROM outbox
 WHERE run_id=$1 AND mercury_job_id=$2 AND submitted_at IS NOT NULL
 ON CONFLICT (run_id) DO NOTHING RETURNING run_id`, string(value.RunID()), value.MercuryJobID(), string(value.State()), value.ObservedAt()).Scan(&inserted)
	if err == nil {
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "23503" && (pe.ConstraintName == "run_terminals_run_fk" || pe.ConstraintName == "run_terminals_outbox_job_fk") {
			err = ErrRunNotSubmitted
		}
		return fmt.Errorf("record run terminal: insert: %w", err)
	}
	stored, err := r.GetByRunID(ctx, value.RunID())
	if err == nil {
		if stored.MercuryJobID() != value.MercuryJobID() || stored.State() != value.State() {
			return ErrTerminalConflict
		}
		return nil
	}
	if !errors.Is(err, ErrTerminalNotFound) {
		return err
	}
	var job *string
	err = r.db.QueryRow(ctx, `SELECT mercury_job_id FROM outbox WHERE run_id=$1`, string(value.RunID())).Scan(&job)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && job == nil) {
		return ErrRunNotSubmitted
	}
	if err != nil {
		return fmt.Errorf("record run terminal: read association: %w", err)
	}
	if *job != value.MercuryJobID() {
		return ErrTerminalConflict
	}
	return errors.New("record run terminal: acknowledged observation was not visible")
}

func (r *TerminalRepository) GetByRunID(ctx context.Context, id delivery.RunID) (delivery.RunTerminal, error) {
	var run, job, state string
	var at time.Time
	err := r.db.QueryRow(ctx, `SELECT run_id,mercury_job_id,state,observed_at FROM run_terminals WHERE run_id=$1`, string(id)).Scan(&run, &job, &state, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrTerminalNotFound
	}
	if err != nil {
		return delivery.RunTerminal{}, fmt.Errorf("get run terminal: %w", err)
	}
	value, err := delivery.NewRunTerminal(delivery.RunID(run), job, delivery.TerminalState(state), at)
	if err != nil {
		return delivery.RunTerminal{}, fmt.Errorf("get run terminal: reconstruct: %w", err)
	}
	return value, nil
}

// ListUnresolvedAfter pages acknowledged runs, including those still executing.
// Cursor ordering is creation time then C-collated run ID. Reads do not claim
// work; all rows close before callers inspect Mercury. Pending acknowledgments
// belong to the dispatcher and are excluded, as are already reconciled runs.
func (r *TerminalRepository) ListUnresolvedAfter(ctx context.Context, limit int, after time.Time, id delivery.RunID) ([]delivery.SubmittedRun, error) {
	if limit < 1 || limit > MaxReconciliationBatch || (after.IsZero() != (id == "")) {
		return nil, errors.New("list unreconciled runs: invalid limit or cursor")
	}
	var cursor any
	if !after.IsZero() {
		cursor = after
	}
	rows, err := r.db.Query(ctx, `SELECT r.id,r.delivery_id,r.created_at,r.trigger,o.mercury_job_id
 FROM delivery_runs r JOIN outbox o ON o.run_id=r.id
 WHERE o.mercury_job_id IS NOT NULL AND o.submitted_at IS NOT NULL
 AND NOT EXISTS (SELECT 1 FROM run_terminals t WHERE t.run_id=r.id)
 AND ($2::timestamptz IS NULL OR r.created_at>$2 OR (r.created_at=$2 AND r.id COLLATE "C">$3 COLLATE "C"))
 ORDER BY r.created_at,r.id COLLATE "C" LIMIT $1`, limit, cursor, string(id))
	if err != nil {
		return nil, fmt.Errorf("list unreconciled runs: %w", err)
	}
	defer rows.Close()
	result := make([]delivery.SubmittedRun, 0)
	for rows.Next() {
		var run, d, trigger, job string
		var at time.Time
		if err := rows.Scan(&run, &d, &at, &trigger, &job); err != nil {
			return nil, fmt.Errorf("list unreconciled runs: scan: %w", err)
		}
		value, err := delivery.NewRun(delivery.RunID(run), delivery.ID(d), at, delivery.Trigger(trigger))
		if err != nil {
			return nil, fmt.Errorf("list unreconciled runs: reconstruct: %w", err)
		}
		result = append(result, delivery.SubmittedRun{Run: value, MercuryJobID: job})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list unreconciled runs: iterate: %w", err)
	}
	return result, nil
}
