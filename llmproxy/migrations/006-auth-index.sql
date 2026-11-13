-- Exact credential lookup index: authentication is a point query on
-- (type, secret), not the operator CLI's prefix scan.
CREATE INDEX IF NOT EXISTS api_key_type_secret
    ON api_key(type, secret);
