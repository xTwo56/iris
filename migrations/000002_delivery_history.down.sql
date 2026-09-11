BEGIN;

-- Remove only delivery history, dependents first. No CASCADE: unexpected
-- dependencies must stop rollback. Existing event and routing tables remain.
DROP TABLE attempt_outcomes;
DROP TABLE attempt_starts;
DROP TABLE delivery_runs;
DROP TABLE deliveries;

COMMIT;
