BEGIN;
-- Existing endpoints are intentionally not backfilled. Missing secrets block
-- signed delivery until the user creates a replacement endpoint.
CREATE TABLE endpoint_secrets (
    endpoint_id text PRIMARY KEY CHECK (endpoint_id ~ '[^[:space:]]'),
    encryption_format integer NOT NULL CHECK (encryption_format = 1),
    nonce bytea NOT NULL CHECK (octet_length(nonce) = 12),
    ciphertext bytea NOT NULL CHECK (octet_length(ciphertext) = 48),
    CONSTRAINT endpoint_secrets_endpoint_fk FOREIGN KEY (endpoint_id)
        REFERENCES endpoints (id) ON DELETE RESTRICT
);
COMMENT ON TABLE endpoint_secrets IS 'One AES-256-GCM encrypted 32-byte HMAC secret per endpoint. Runtime encryption keys and plaintext secrets are never stored here. Endpoint creation inserts both records in one transaction.';
COMMENT ON COLUMN endpoint_secrets.ciphertext IS '32 encrypted bytes plus 16-byte authentication tag. Associated data binds format version and endpoint identity; nonce is freshly randomized per encryption.';
COMMIT;
