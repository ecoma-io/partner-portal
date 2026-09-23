-- Partner Portal Ledger Schema
-- SQLite with WAL mode for durability

-- Raw usage records (one per request)
CREATE TABLE IF NOT EXISTS usage_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,  -- ISO 8601 timestamp
    consumer_id TEXT NOT NULL,
    model TEXT NOT NULL,
    endpoint TEXT NOT NULL,  -- 'chat_completions', 'responses', 'models'
    streaming INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    request_status TEXT NOT NULL,  -- 'completed', 'failed', 'interrupted'
    input_tokens INTEGER,
    output_tokens INTEGER,
    cached_tokens INTEGER,
    ttft_ms INTEGER,  -- Time to first token (streaming only)
    duration_ms INTEGER NOT NULL,
    usage_status TEXT NOT NULL,  -- 'available', 'unavailable', 'partial'
    error_message TEXT,

    -- Indexes for common query patterns
    CHECK (request_status IN ('completed', 'failed', 'interrupted')),
    CHECK (usage_status IN ('available', 'unavailable', 'partial')),
    CHECK (endpoint IN ('chat_completions', 'responses', 'models'))
);

CREATE INDEX IF NOT EXISTS idx_usage_records_created_at ON usage_records(created_at);
CREATE INDEX IF NOT EXISTS idx_usage_records_consumer_id ON usage_records(consumer_id);
CREATE INDEX IF NOT EXISTS idx_usage_records_consumer_created ON usage_records(consumer_id, created_at);
CREATE INDEX IF NOT EXISTS idx_usage_records_request_status ON usage_records(request_status);

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
    ttft_count INTEGER NOT NULL DEFAULT 0,  -- Count of requests with TTFT
    success_count INTEGER NOT NULL DEFAULT 0,
    failure_count INTEGER NOT NULL DEFAULT 0,

    UNIQUE(hour, consumer_id, model, endpoint, streaming)
);

CREATE INDEX IF NOT EXISTS idx_usage_hourly_hour ON usage_hourly(hour);
CREATE INDEX IF NOT EXISTS idx_usage_hourly_consumer_hour ON usage_hourly(consumer_id, hour);

-- Metadata table for tracking schema version and maintenance
CREATE TABLE IF NOT EXISTS ledger_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Initialize metadata
INSERT OR IGNORE INTO ledger_meta (key, value) VALUES
    ('schema_version', '1'),
    ('last_retention_run', NULL),
    ('last_vacuum', NULL);
