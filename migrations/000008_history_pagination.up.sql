BEGIN;
-- Parent equality followed by immutable timestamp and bytewise identity supports
-- bounded history pages without sorting the complete parent history. Existing
-- uniqueness and reconciliation indexes do not have these parent/order prefixes.
CREATE INDEX deliveries_history_order ON deliveries (event_id, created_at, id COLLATE "C");
CREATE INDEX delivery_runs_history_order ON delivery_runs (delivery_id, created_at, id COLLATE "C");
CREATE INDEX attempt_starts_history_order ON attempt_starts (run_id, started_at, id COLLATE "C");
COMMIT;
