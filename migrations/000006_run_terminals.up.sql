BEGIN;

-- Bind the terminal observation to the acknowledged job, not merely to a run.
-- Pending outbox rows have NULL job IDs and cannot satisfy the composite FK.
ALTER TABLE outbox ADD CONSTRAINT outbox_run_job_key UNIQUE (run_id, mercury_job_id);

CREATE TABLE run_terminals (
    run_id text PRIMARY KEY CHECK (run_id ~ '[^[:space:]]'),
    mercury_job_id text NOT NULL CHECK (mercury_job_id ~ '[^[:space:]]'),
    state text NOT NULL CHECK (state IN ('succeeded', 'failed')),
    observed_at timestamptz NOT NULL CHECK (
        isfinite(observed_at) AND observed_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    CONSTRAINT run_terminals_run_fk FOREIGN KEY (run_id)
        REFERENCES delivery_runs (id) ON DELETE RESTRICT,
    CONSTRAINT run_terminals_outbox_job_fk FOREIGN KEY (run_id, mercury_job_id)
        REFERENCES outbox (run_id, mercury_job_id) ON DELETE RESTRICT
);
CREATE INDEX delivery_runs_reconciliation_order ON delivery_runs (created_at, id COLLATE "C");

COMMENT ON TABLE run_terminals IS 'Iris observation of a confirmed terminal Mercury job. Absence means not yet reconciled, not necessarily running. Repository operations only insert/read; SQL privileges, not this comment, govern direct UPDATE/DELETE.';
COMMENT ON COLUMN run_terminals.state IS 'Mercury succeeded means completed successfully; failed includes permanent failure or exhausted retries. Neither state reconstructs missing HTTP attempt outcomes or proves whether the receiver processed a request.';
COMMENT ON COLUMN run_terminals.observed_at IS 'When Iris observed the terminal state; not an invented Mercury completion time. Repeated observations preserve the first stored record.';

COMMIT;
