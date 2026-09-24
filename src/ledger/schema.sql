-- Partner Portal Ledger Schema
--
-- SQLite in WAL mode with synchronous=FULL. The raw ledger is the source of
-- truth; `usage_hourly` is a derived rollup that is always written in the same
-- transaction as its raw row (BEGIN -> raw -> rollup -> COMMIT).
--
-- Lifecycle: a request is INSERTed as 'in_flight' before the upstream is
-- contacted, then moved to a terminal state. A row still in 'in_flight' whose
-- owning instance is gone is recovered to 'interrupted' (see recovery.rs). Only
-- terminal states are ever rolled up.
--
-- Every statement here is additive and idempotent (`CREATE ... IF NOT EXISTS`,
-- `INSERT OR IGNORE`), because this file is executed against both a fresh
-- database and an existing one. Statements that cannot be idempotent in SQL —
-- adding a column to a table that may already exist — live in `init_schema`,
-- which applies them only when the recorded version says they are missing.

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
    -- Owning instance, recorded so that recovery can tell "stranded by a dead
    -- process" from "still running in a live one" (same-VPS rolling update).
    -- NULL means the row predates instance ownership; those rows are only
    -- recoverable once they are old enough to rule out a live owner.
    instance_id TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cached_tokens INTEGER,
    ttft_ms INTEGER,  -- Time to first token (streaming only)
    duration_ms INTEGER NOT NULL,
    usage_status TEXT NOT NULL,
    error_message TEXT,
    -- Raw upstream error text only: bounded to 8 KiB by the proxy. It is NULL
    -- for requests, successful responses, and streaming responses.
    error_body TEXT,

    CHECK (request_status IN ('in_flight', 'completed', 'failed', 'interrupted')),
    CHECK (usage_status IN ('available', 'unavailable', 'partial')),
    CHECK (endpoint IN ('chat_completions', 'responses', 'models')),
    -- Token columns are either absent or non-negative. Never negative, and
    -- never a fabricated zero standing in for "unknown" (unknown stays NULL).
    CHECK (input_tokens IS NULL OR input_tokens >= 0),
    CHECK (output_tokens IS NULL OR output_tokens >= 0),
    CHECK (cached_tokens IS NULL OR cached_tokens >= 0)
);

CREATE INDEX IF NOT EXISTS idx_usage_records_created_at ON usage_records(created_at);

CREATE INDEX IF NOT EXISTS idx_usage_records_consumer_created_id
    ON usage_records(consumer_id, created_at, id);

-- Registrations of running instances. A row here means "this instance booted
-- against this database"; liveness is decided by the lock file named in
-- `instance.rs`, not by this table, so a crashed instance leaves a row behind
-- that the next recovery sweeps away.
CREATE TABLE IF NOT EXISTS ledger_instances (
    instance_id TEXT PRIMARY KEY,
    booted_at TEXT NOT NULL,
    pid INTEGER,
    host TEXT
);

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

-- Metadata table for tracking schema version and maintenance.
--
-- `schema_version` is deliberately NOT seeded here. `init_schema` reads whatever
-- is stored, compares it against the version it supports, and writes the new
-- value only after the migration has actually been applied — so a database
-- created by an older build can never be relabelled as current.
CREATE TABLE IF NOT EXISTS ledger_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Signing key for opaque dashboard pagination cursors. Generated once per
-- database, so it survives a rolling update (both instances must accept the
-- other's cursors) and is never any process's configuration secret.
INSERT OR IGNORE INTO ledger_meta (key, value)
    VALUES ('cursor_key', hex(randomblob(32)));

INSERT OR IGNORE INTO ledger_meta (key, value) VALUES
    ('last_retention_run', ''),
    ('last_recovery_run', '');
