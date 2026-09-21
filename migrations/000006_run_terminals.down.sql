BEGIN;
DROP TABLE run_terminals;
DROP INDEX delivery_runs_reconciliation_order;
ALTER TABLE outbox DROP CONSTRAINT outbox_run_job_key;
COMMIT;
