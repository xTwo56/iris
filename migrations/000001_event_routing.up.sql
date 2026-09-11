BEGIN;

-- tables belong to the webhook service and its database.
-- caller supplied IDs and timestamps
CREATE TABLE events (
    id text PRIMARY KEY CHECK (id ~ '[^[:space:]]'),
    event_type text NOT NULL CHECK (event_type ~ '[^[:space:]]'),
    created_at timestamptz NOT NULL CHECK (
        isfinite(created_at) AND created_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    payload bytea NOT NULL
);

-- '[^[:space:]]' : It rejects blank attributes; it does not prohibit whitespace inside or around
-- an otherwise nonblank ID
CREATE TABLE endpoints (
    id text PRIMARY KEY CHECK (id ~ '[^[:space:]]'), 
    url text NOT NULL CHECK (url ~ '[^[:space:]]'),
    created_at timestamptz NOT NULL CHECK (
        isfinite(created_at) AND created_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    fanout boolean NOT NULL DEFAULT true
);

CREATE TABLE subscriptions (
    id text PRIMARY KEY CHECK (id ~ '[^[:space:]]'),
    endpoint_id text NOT NULL CHECK (endpoint_id ~ '[^[:space:]]'),
    event_type text NOT NULL CHECK (event_type ~ '[^[:space:]]'),
    created_at timestamptz NOT NULL CHECK (
        isfinite(created_at) AND created_at <> TIMESTAMPTZ '0001-01-01 00:00:00+00'
    ),
    enabled boolean NOT NULL DEFAULT true,
    CONSTRAINT subscriptions_endpoint_fk FOREIGN KEY (endpoint_id)
        REFERENCES endpoints (id) ON DELETE RESTRICT,
    CONSTRAINT subscriptions_endpoint_event_type_key UNIQUE (endpoint_id, event_type)
);

COMMIT;
