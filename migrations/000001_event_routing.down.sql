BEGIN;

-- Roll back only this service's initial schema, dependents first. No CASCADE:
-- unexpected dependencies must stop rollback rather than be silently removed.
DROP TABLE subscriptions;
DROP TABLE endpoints;
DROP TABLE events;

COMMIT;
