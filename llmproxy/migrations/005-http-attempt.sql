CREATE TABLE http_attempt (
    attempt_id TEXT NOT NULL PRIMARY KEY,
    binding TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'prepared'
        CHECK (state IN ('prepared', 'unknown', 'confirmed', 'billed', 'rejected')),
    resources TEXT,
    observed_at TEXT
);
CREATE INDEX http_attempt_pending ON http_attempt(state, attempt_id);
CREATE TABLE http_attempt_financial (
    attempt_id TEXT NOT NULL REFERENCES http_attempt(attempt_id),
    product TEXT NOT NULL,
    financial_id INTEGER NOT NULL UNIQUE REFERENCES event_oneoff(id),
    PRIMARY KEY (attempt_id, product)
);
