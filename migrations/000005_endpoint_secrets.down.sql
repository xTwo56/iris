BEGIN;
-- Removing ciphertext loses signing credentials; preserve endpoints, no CASCADE.
DROP TABLE endpoint_secrets;
COMMIT;
