BEGIN;
-- Remove submission storage only; no cascading removal of other records.
DROP TABLE outbox;
COMMIT;
