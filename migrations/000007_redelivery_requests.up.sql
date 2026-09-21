BEGIN;

CREATE TABLE redelivery_requests (
    submission_key text PRIMARY KEY CHECK (submission_key ~ '[^[:space:]]'),
    requested_delivery_id text NOT NULL CHECK (requested_delivery_id ~ '[^[:space:]]'),
    run_id text UNIQUE CHECK (run_id ~ '[^[:space:]]'),
    CONSTRAINT redelivery_requests_run_fk FOREIGN KEY (run_id)
        REFERENCES delivery_runs (id) ON DELETE RESTRICT
);
COMMENT ON TABLE redelivery_requests IS 'Manual-redelivery request keys, scoped across Iris independently of producer submission keys and Mercury outbox keys. Acquisition and result completion share the run/outbox transaction. Application code must never commit an incomplete result. No expiration.';
COMMENT ON COLUMN redelivery_requests.requested_delivery_id IS 'Exact caller input, compared before delivery lookup or unresolved-run checks. Intentionally not a foreign key: acquiring the key must not lock a referenced delivery before the application takes its serialization lock. The application validates existence and run ownership within the same transaction.';
COMMENT ON COLUMN redelivery_requests.run_id IS 'Original committed result. NULL only while acquiring a new request in its uncommitted transaction. Replay returns this run even while it is unresolved.';

COMMIT;
