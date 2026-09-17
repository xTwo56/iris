BEGIN;

-- Iris owns submission intent; event bodies and endpoint configuration stay in
-- their existing tables. One entry belongs to one execution run.
CREATE TABLE outbox (
    run_id text PRIMARY KEY CHECK (run_id ~ '[^[:space:]]'),
    submission_key text NOT NULL CHECK (submission_key ~ '[^[:space:]]'),
    created_at timestamptz NOT NULL CHECK (
        isfinite(created_at) AND created_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    mercury_job_id text CHECK (mercury_job_id ~ '[^[:space:]]'),
    submitted_at timestamptz CHECK (
        isfinite(submitted_at) AND submitted_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    CONSTRAINT outbox_run_fk FOREIGN KEY (run_id) REFERENCES delivery_runs (id) ON DELETE RESTRICT,
    CONSTRAINT outbox_submission_key_key UNIQUE (submission_key),
    CONSTRAINT outbox_ack_pair_check CHECK (
        (mercury_job_id IS NULL AND submitted_at IS NULL)
        OR (mercury_job_id IS NOT NULL AND submitted_at IS NOT NULL)
    )
);
CREATE INDEX outbox_pending_order ON outbox (created_at, run_id COLLATE "C")
    WHERE mercury_job_id IS NULL;
COMMENT ON TABLE outbox IS 'Insert submission intent in the same transaction as run creation. Pending reads do not reserve records. Submitted means Mercury acknowledged a job, not webhook delivery success.';
COMMENT ON COLUMN outbox.submission_key IS 'Caller-supplied immutable idempotency key; future dispatchers must reuse it after crashes or uncertain HTTP responses. Immutability is enforced by repository operations, not this comment.';
COMMIT;
