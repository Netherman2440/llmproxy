CREATE TABLE IF NOT EXISTS admission_lease (
    bucket TEXT NOT NULL,
    owner TEXT NOT NULL,
    expires_at REAL NOT NULL,
    PRIMARY KEY (bucket, owner)
);
CREATE INDEX IF NOT EXISTS admission_lease_expiry ON admission_lease(expires_at);
