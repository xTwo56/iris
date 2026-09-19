BEGIN;

CREATE TABLE event_submissions (
    submission_key text PRIMARY KEY CHECK (submission_key ~ '[^[:space:]]'),
    request_data bytea NOT NULL,
    event_id text CHECK (event_id ~ '[^[:space:]]'),
    delivery_count integer CHECK (delivery_count >= 0),
    CONSTRAINT event_submissions_event_fk FOREIGN KEY (event_id)
        REFERENCES events (id) ON DELETE RESTRICT,
    CONSTRAINT event_submissions_result_pair CHECK (
        (event_id IS NULL AND delivery_count IS NULL)
        OR (event_id IS NOT NULL AND delivery_count IS NOT NULL)
    )
);
COMMENT ON TABLE event_submissions IS 'Iris producer request deduplication, separate from Mercury outbox keys. Key acquisition and result completion share the acceptance transaction; application code must never commit an incomplete result. No expiration.';
COMMENT ON COLUMN event_submissions.request_data IS 'Versioned length-prefixed exact request encoding: supplied event ID, type, UTC timestamp at nanosecond precision, and original payload bytes. No internally generated work fields.';
COMMENT ON COLUMN event_submissions.delivery_count IS 'Original committed fan-out count, including zero; replays do not re-evaluate routing.';

COMMIT;
