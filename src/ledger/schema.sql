-- Partner Portal Ledger Schema
--
-- SQLite in WAL mode with synchronous=FULL. The raw ledger is the source of
-- truth; `usage_hourly` is a derived rollup that is always written in the same
-- transaction as its raw row (BEGIN -> raw -> rollup -> COMMIT).
--
-- Lifecycle: a request is INSERTed as 'in_flight' before the upstream is
-- contacted, then UPSERTed to a terminal state. A row still in 'in_flight' at
-- startup is a request whose process died mid-flight and is recovered to
-- 'interrupted' (see recovery.rs). Only terminal states are rolled up.

CREATE TABLE IF NOT EXISTS usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,  -- ISO 8601 timestamp (UTC)
    consumer_id TEXT NOT NULL,
    model TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    streaming INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    request_status TEXT NOT NULL,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cached_tokens INTEGER,
    ttft_ms INTEGER,  -- Time to first token (streaming only)
    duration_ms INTEGER NOT NULL,
    usage_status TEXT NOT NULL,
    error_message TEXT,

    CHECK (request_status IN ('in_flight', 'completed', 'failed', 'interrupted')),
    CHECK (usage_status IN ('available', 'unavailable', 'partial')),
    CHECK (endpoint IN ('chat_completions', 'responses', 'models')),
    -- Token columns are either absent or non-negative. Never negative, and
    -- never a fabricated zero standing in for "unknown" (unknown stays NULL).
    CHECK (input_tokens IS NULL OR input_tokens >= 0),
    CHECK (output_tokens IS NULL OR output_tokens >= 0),
    CHECK (cached_tokens IS NULL OR cached_tokens >= 0)
);

-- Startup recovery scan: find rows left in flight by a dead process.
CREATE INDEX IF NOT EXISTS idx_usage_records_in_flight
    ON usage_records(request_status) WHERE request_status = 'in_flight';

-- Retention sweep: delete oldest rows by time.
CREATE INDEX IF NOT EXISTS idx_usage_records_created_at ON usage_records(created_at);

-- Dashboard keyset pagination and per-consumer range scans. Ordering is
-- (created_at DESC, id DESC), so the index carries id to keep the sort
-- index-only and avoid a filesort.
CREATE INDEX IF NOT EXISTS idx_usage_records_consumer_created_id
    ON usage_records(consumer_id, created_at, id);

-- Hourly aggregates for efficient dashboard queries
CREATE TABLE IF NOT EXISTS usage_hourly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hour TEXT NOT NULL,  -- ISO 8601 hour: '2024-01-15T10'
    consumer_id TEXT NOT NULL,
    model TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    streaming INTEGER NOT NULL DEFAULT 0,

    -- Aggregates
    request_count INTEGER NOT NULL DEFAULT 0,
    total_input_tokens INTEGER NOT NULL DEFAULT 0,
    total_output_tokens INTEGER NOT NULL DEFAULT 0,
    total_cached_tokens INTEGER NOT NULL DEFAULT 0,
    total_duration_ms INTEGER NOT NULL DEFAULT 0,
    total_ttft_ms INTEGER NOT NULL DEFAULT 0,
    ttft_count INTEGER NOT NULL DEFAULT 0,  -- Count of requests that reported TTFT
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,  -- failed + interrupted

    UNIQUE(hour, consumer_id, model, endpoint, streaming)
);

CREATE INDEX IF NOT EXISTS idx_usage_hourly_hour ON usage_hourly(hour);
CREATE INDEX IF NOT EXISTS idx_usage_hourly_consumer_hour ON usage_hourly(consumer_id, hour);

-- Metadata table for tracking schema version and maintenance
CREATE TABLE IF NOT EXISTS ledger_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

INSERT OR IGNORE INTO ledger_meta (key, value) VALUES
    ('schema_version', '2'),
    ('last_retention_run', ''),
    ('last_recovery_run', '');
