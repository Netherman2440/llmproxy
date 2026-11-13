CREATE TABLE conversion_delivery (
    delivery_key TEXT PRIMARY KEY,
    binding TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'prepared',
    financial_id INTEGER UNIQUE REFERENCES event_oneoff(id)
);
CREATE INDEX conversion_delivery_pending ON conversion_delivery(state, delivery_key);
