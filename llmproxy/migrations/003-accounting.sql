ALTER TABLE event_oneoff ADD COLUMN quantity_exact TEXT;
CREATE TABLE accounting_session (
    session_id TEXT PRIMARY KEY,
    binding TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    backend_instance_id TEXT,
    reconcile_cursor INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    last_error TEXT
);
CREATE TABLE accounting_receipt (
    event_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES accounting_session(session_id),
    backend_instance_id TEXT NOT NULL,
    cursor INTEGER NOT NULL,
    segment_id TEXT NOT NULL,
    start_sample INTEGER NOT NULL,
    sample_count INTEGER NOT NULL,
    fingerprint TEXT NOT NULL,
    receipt TEXT NOT NULL,
    status TEXT NOT NULL,
    billing_event_id INTEGER UNIQUE REFERENCES event_oneoff(id),
    quantity_exact TEXT,
    acked_at TEXT,
    UNIQUE(backend_instance_id, cursor),
    UNIQUE(session_id, segment_id)
);
CREATE INDEX accounting_receipt_session ON accounting_receipt(session_id, acked_at);
CREATE INDEX accounting_session_pending ON accounting_session(status, session_id);
