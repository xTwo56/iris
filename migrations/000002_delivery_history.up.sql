BEGIN;

-- Iris owns these delivery obligations and request history. IDs and timestamps
-- are caller supplied. Foreign keys retain referenced records without cascades.
CREATE TABLE deliveries (
    id text PRIMARY KEY CHECK (id ~ '[^[:space:]]'),
    event_id text NOT NULL CHECK (event_id ~ '[^[:space:]]'),
    endpoint_id text NOT NULL CHECK (endpoint_id ~ '[^[:space:]]'),
    created_at timestamptz NOT NULL CHECK (
        isfinite(created_at) AND created_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    CONSTRAINT deliveries_event_fk FOREIGN KEY (event_id)
        REFERENCES events (id) ON DELETE RESTRICT,
    CONSTRAINT deliveries_endpoint_fk FOREIGN KEY (endpoint_id)
        REFERENCES endpoints (id) ON DELETE RESTRICT,
    CONSTRAINT deliveries_event_endpoint_key UNIQUE (event_id, endpoint_id)
);

CREATE TABLE delivery_runs (
    id text PRIMARY KEY CHECK (id ~ '[^[:space:]]'),
    delivery_id text NOT NULL CHECK (delivery_id ~ '[^[:space:]]'),
    created_at timestamptz NOT NULL CHECK (
        isfinite(created_at) AND created_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    trigger text NOT NULL CHECK (trigger IN ('initial', 'manual_redelivery')),
    CONSTRAINT delivery_runs_delivery_fk FOREIGN KEY (delivery_id)
        REFERENCES deliveries (id) ON DELETE RESTRICT
);
CREATE UNIQUE INDEX delivery_runs_one_initial_per_delivery
    ON delivery_runs (delivery_id) WHERE trigger = 'initial';

CREATE TABLE attempt_starts (
    id text PRIMARY KEY CHECK (id ~ '[^[:space:]]'),
    run_id text NOT NULL CHECK (run_id ~ '[^[:space:]]'),
    started_at timestamptz NOT NULL CHECK (
        isfinite(started_at) AND started_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    CONSTRAINT attempt_starts_run_fk FOREIGN KEY (run_id)
        REFERENCES delivery_runs (id) ON DELETE RESTRICT
);

CREATE TABLE attempt_outcomes (
    attempt_id text PRIMARY KEY CHECK (attempt_id ~ '[^[:space:]]'),
    finished_at timestamptz NOT NULL CHECK (
        isfinite(finished_at) AND finished_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    http_status integer CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    classification text NOT NULL CHECK (
        classification IN ('succeeded', 'retryable_failure', 'permanent_failure')
    ),
    CONSTRAINT attempt_outcomes_start_fk FOREIGN KEY (attempt_id)
        REFERENCES attempt_starts (id) ON DELETE RESTRICT,
    -- CHECK accepts UNKNOWN, so success must explicitly require a present status.
    CONSTRAINT attempt_outcomes_status_classification_check CHECK (
        (classification = 'succeeded' AND http_status IS NOT NULL AND http_status BETWEEN 200 AND 299)
        OR
        (classification IN ('retryable_failure', 'permanent_failure')
            AND (http_status IS NULL OR http_status NOT BETWEEN 200 AND 299))
    )
);

COMMIT;
