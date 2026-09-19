BEGIN;
-- Remove producer submission records only, without cascading to accepted events.
DROP TABLE event_submissions;
COMMIT;
