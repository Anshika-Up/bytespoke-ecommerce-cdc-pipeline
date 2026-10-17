PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS raw_events (
    event_id TEXT PRIMARY KEY,
    entity TEXT NOT NULL CHECK (entity IN ('customer', 'order')),
    operation TEXT NOT NULL CHECK (operation IN ('INSERT', 'UPDATE')),
    entity_id TEXT NOT NULL,
    event_time TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    email TEXT,
    city TEXT,
    last_event_time TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    amount REAL NOT NULL CHECK (amount >= 0),
    status TEXT NOT NULL,
    last_event_time TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS change_summaries (
    summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    event_count INTEGER NOT NULL,
    summary_text TEXT NOT NULL,
    provider TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_events_entity_time
    ON raw_events(entity, event_time);
