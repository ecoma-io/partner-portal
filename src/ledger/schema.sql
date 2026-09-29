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
    -- The prices in force when this request was ACCEPTED, in micro-USD per
    -- million tokens (billing; see docs/adr/0015).
    --
    -- A price, not a cost: the formula that turns these into money is
    -- documented and stable, so storing the inputs keeps a statement
    -- re-derivable, and a price change during the day produces two snapshots
    -- inside one statement instead of silently repricing the earlier one.
    --
    -- NULL means "this request was accepted with no billing configuration" —
    -- a request metered before billing existed, or one whose model had no
    -- complete price set. It is NOT zero and must never become zero: a missing
    -- price is what makes a day "billing-incomplete", and a fabricated 0 would
    -- turn "we do not know what this cost" into "this was free".
    --
    -- All three are written together by one statement in `writer.rs` and are
    -- either all present or all NULL. That is enforced by construction rather
    -- than by a table-level CHECK, because a CHECK spanning three columns can
    -- only be added by rebuilding `usage_records` — the table the metering path
    -- writes on every request. A trigger would add a statement to the measured
    -- hot path to guard a property the single writer already cannot violate;
    -- `test_price_snapshot_is_written_whole_or_not_at_all` is the guard instead.
    input_price_snapshot INTEGER,
    cached_input_price_snapshot INTEGER,
    output_price_snapshot INTEGER,

    CHECK (request_status IN ('in_flight', 'completed', 'failed', 'interrupted')),
    CHECK (usage_status IN ('available', 'unavailable', 'partial')),
    CHECK (endpoint IN ('chat_completions', 'responses', 'models')),
    -- Token columns are either absent or non-negative. Never negative, and
    -- never a fabricated zero standing in for "unknown" (unknown stays NULL).
    CHECK (input_tokens IS NULL OR input_tokens >= 0),
    CHECK (output_tokens IS NULL OR output_tokens >= 0),
    CHECK (cached_tokens IS NULL OR cached_tokens >= 0),
    -- Same rule for the price snapshots: a price may legitimately be zero (an
    -- operator configured the model as free) but can never be negative.
    CHECK (input_price_snapshot IS NULL OR input_price_snapshot >= 0),
    CHECK (cached_input_price_snapshot IS NULL OR cached_input_price_snapshot >= 0),
    CHECK (output_price_snapshot IS NULL OR output_price_snapshot >= 0)
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

-- Partner API keys. This table is the source of truth for who may call the
-- proxy; `config.yaml` carries no keys (ADR 0014).
--
-- The plaintext is NEVER stored. `key_hash` is hex(HMAC-SHA256(secret, key))
-- where `secret` is `PARTNER_PORTAL_API_KEY_SECRET` from the runtime
-- environment — keyed, not a bare digest, so a stolen database cannot be
-- brute-forced offline without it. `key_prefix` is the first few characters of
-- the plaintext and exists so an operator can identify a key in a list; it is
-- not a secret and is not used to authenticate anything.
--
-- There is deliberately no model column. Which models a partner may call, and
-- what each costs, is one list in `partner_models` (ADR 0015); a second list
-- here would be a second answer to the same question, and the two would
-- eventually disagree on a customer's invoice. A v5 database carried
-- `allowed_models` here; the v6 migration copies it into `partner_models` and
-- drops it.
--
-- `status` is two-state on purpose. Expiry is not a third state: a key past
-- `expires_at` is simply not loaded into the authentication snapshot, so the
-- question "is this key still good" is answered by one predicate at load time
-- instead of by a clock check on the request path.
CREATE TABLE IF NOT EXISTS api_keys (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL,
    -- The identity every ledger row is scoped by. Taken from the credential,
    -- never from the request (ADR 0008).
    consumer_id    TEXT NOT NULL,
    key_prefix     TEXT NOT NULL,
    key_hash       TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'active',
    created_at     TEXT NOT NULL,
    updated_at     TEXT NOT NULL,
    expires_at     TEXT,
    revoked_at     TEXT,

    UNIQUE (key_hash),
    CHECK (status IN ('active', 'revoked')),
    -- A revoked key must say when it was revoked, and a live one must not carry
    -- a revocation that never happened.
    CHECK ((status = 'active' AND revoked_at IS NULL)
        OR (status = 'revoked' AND revoked_at IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(status);

-- One partner, one live credential.
--
-- A partner is a commercial account, not a set of credentials: an operator
-- rotates a key by revoking the old row and inserting the new one in a single
-- transaction (ADR 0014), so a partner is never legitimately in a state with
-- two keys that both work. The constraint is a partial unique index rather than
-- application logic, because "check whether one already exists, then insert"
-- is a race: two instances serving two concurrent create requests would both
-- see zero rows and both insert. Revoked rows are outside the index, so the
-- history of every key a partner ever had is still retained.
--
-- `api_keys.consumer_id` is the foreign identity here only in the logical
-- sense: this table is written by the key lifecycle before a `partners` row
-- exists (the bootstrap case in docs/adr/0015), so no FOREIGN KEY is declared
-- and the check that keeps a partner and its keys consistent lives in the key
-- store, which is the only writer.
--
-- A note on `idx_api_keys_one_active_per_consumer` and the order things run in:
-- the migration in `src/ledger/mod.rs` resolves a pre-existing v5 database's
-- duplicate active keys *before* this file is executed, because creating the
-- index here is the first statement that would fail on them. Nothing in the
-- migration may assume this constraint is absent.
CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_one_active_per_consumer
    ON api_keys(consumer_id) WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_api_keys_consumer ON api_keys(consumer_id);

-- ---------------------------------------------------------------------------
-- Billing (docs/adr/0015). These tables are financial history, not usage:
-- `database.retention_days` prunes `usage_records` and `usage_hourly` and
-- must never touch anything below, because a statement is the record a partner
-- is asked to pay against long after the usage it summarises is gone.
-- ---------------------------------------------------------------------------

-- The commercial unit. `consumer_id` is the primary key on purpose: it is the
-- identity boundary the ledger is already scoped by (ADR 0008), and a partner
-- must not be able to widen, narrow or rename its own account by editing a
-- request. The ledger keeps saying `consumer_id`; only this module and the
-- business surface call it a partner.
--
-- `billing_mode` is the whole reason this is not a price of 0 or a `due_at`
-- of 9999-12-31:
--
--   * `invoice`       — this partner owes money. A statement has a `due_at`
--                       from `payment_terms_minutes`, and a complete unpaid
--                       statement past its due date suspends service.
--   * `reconciliation`— marketplace/settlement partner. Statements are issued
--                       and kept long-term for the settlement record, and carry
--                       no payment obligation at all: no due date is enforced,
--                       no email is sent, and the service is never suspended
--                       for a non-payment. Modelling this as "unlimited", as a
--                       zero price, or as a statement that reads `unpaid` would
--                       be a misstatement in a document a partner relies on.
--
-- `service_status` is deliberately NOT a column. Suspension is *derived*
-- (see `src/billing/status.rs`): a suspended partner and an active partner are
-- the same row, and the answer is a function of statements that already exist.
-- Storing it would mean every transition needs a write on the path that
-- discovers it, and a missed write would either strand a partner or charge one
-- for work delivered. Deriving it means the answer cannot drift from the
-- statements it is computed from.
--
-- `payment_terms_minutes` counts from the END of the billing period, not from
-- the moment the statement is generated, so a partner's clock does not depend
-- on when this process happened to run. It applies to `invoice` only; a
-- reconciliation partner's value is unused, which is why it has a default
-- rather than being nullable.
CREATE TABLE IF NOT EXISTS partners (
    consumer_id           TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    -- Empty means "not configured", and the worker then issues statements
    -- without emailing them rather than refusing to issue them at all. A
    -- missing address is a configuration gap to report, not a reason to skip
    -- the accounting.
    billing_email         TEXT NOT NULL DEFAULT '',
    billing_mode          TEXT NOT NULL DEFAULT 'invoice',
    payment_terms_minutes INTEGER NOT NULL DEFAULT 720,

    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL,

    CHECK (billing_mode IN ('invoice', 'reconciliation')),
    -- Negative terms would put `due_at` before the period it bills for, which
    -- is not "pay immediately", it is a statement that is born overdue.
    CHECK (payment_terms_minutes >= 0)
);

-- Prices, and the ONLY statement of which models a partner may call.
--
-- Capability and price are one row, deliberately. They are the same question —
-- "may this partner run `gpt-4o`, and what does it cost" — and splitting it in
-- two would be two sources of truth that can disagree, with the disagreement
-- only ever discovered on a customer's invoice. This table replaced
-- `api_keys.allowed_models` as the authority (docs/adr/0012, amended by 0015);
-- the authentication snapshot projects both the allow-list and the prices out
-- of it, and the old column is gone rather than emptied.
--
-- Prices are micro-USD per million tokens: `$0.095 / M` is `95000`, and one
-- token at that price is 0.095 micro-USD. Integer because money is an integer;
-- a float price is a rounding bug waiting for a large enough day.
--
-- A row must carry all three prices. There is no "input only" or "cached is
-- the same as input" shorthand: cached input is priced separately by nearly
-- every provider, and defaulting it to the input price would silently over- or
-- under-charge every cached request. Zero is allowed, because an operator may
-- genuinely configure a model as free — but it has to be typed to mean it.
CREATE TABLE IF NOT EXISTS partner_models (
    consumer_id                    TEXT NOT NULL,
    model                          TEXT NOT NULL,

    input_price_micro_usd_per_million       INTEGER NOT NULL,
    cached_input_price_micro_usd_per_million INTEGER NOT NULL,
    output_price_micro_usd_per_million      INTEGER NOT NULL,

    created_at                     TEXT NOT NULL,
    updated_at                     TEXT NOT NULL,

    -- A price belongs to a partner, so it cannot outlive one or precede one.
    -- Without this an orphan row would be a model some future partner with the
    -- same id inherits the price for. `foreign_keys` is ON (see
    -- `configure_sqlite`), so this is enforced rather than decorative.
    FOREIGN KEY (consumer_id) REFERENCES partners(consumer_id) ON DELETE CASCADE,

    UNIQUE (consumer_id, model),
    -- Prices may be zero; they may never be negative, and a negative price
    -- would make a statement total credit a partner money.
    CHECK (input_price_micro_usd_per_million >= 0),
    CHECK (cached_input_price_micro_usd_per_million >= 0),
    CHECK (output_price_micro_usd_per_million >= 0),
    -- No blank model name: the allow-list is compared literally, and an empty
    -- string would be a name no request could ever carry.
    CHECK (length(trim(model)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_partner_models_consumer
    ON partner_models(consumer_id, model);

-- One statement per partner per billing day, and the constraint IS the
-- idempotency guarantee.
--
-- "At most one" is enforced here rather than by an application-level
-- `if not exists` check for the ordinary reason: two instances, a restart, a
-- duplicate worker run and a SIGTERM between the SELECT and the INSERT are all
-- the same race, and a customer being sent two invoices for one day is not a
-- failure the code can absorb. `INSERT ... ON CONFLICT DO NOTHING` against this
-- index is what makes the whole thing safe to run from anywhere, at any time.
--
-- `billing_mode` is copied rather than joined so that a statement reads the
-- way it was issued. Switching a partner to reconciliation must not rewrite
-- what "invoice" meant for a day already closed.
--
-- `total_amount_micro_usd` is the sum of the LINES, not a sum re-computed at
-- read time from `usage_records` — that is the whole reason this table exists
-- (retention prunes the usage it would otherwise be rendered from).
--
-- `incomplete_usage_count` is the number of accepted requests that day which
-- the provider did not report in full, or for which no complete price
-- configuration existed. Their usage is not charged. The count is kept, and not
-- the "just drop them", because an invoice that quietly omits work is worse
-- than one that admits it could not measure some of it.
CREATE TABLE IF NOT EXISTS daily_statements (
    id                        INTEGER PRIMARY KEY AUTOINCREMENT,
    consumer_id               TEXT NOT NULL,
    -- The billing day in the configured billing timezone, 'YYYY-MM-DD'. UTC
    -- timestamps everywhere else in this database; this is the one place a
    -- business calendar exists, and it exists as a label.
    billing_date              TEXT NOT NULL,
    billing_mode              TEXT NOT NULL,
    -- ISO 4217. A single currency for the whole product, held as data so a
    -- second one is a deliberate migration rather than an assumption in code.
    currency                  TEXT NOT NULL DEFAULT 'USD',

    -- The half-open instant range the statement covers, in UTC. Every row it
    -- sums was accepted within [period_start, period_end).
    period_start              TEXT NOT NULL,
    period_end                TEXT NOT NULL,
    -- When this statement was actually issued, which is never `period_end`:
    -- see `close_delay_minutes` in docs/adr/0015. A request accepted inside
    -- the period can finalize after it, and the cut-off is what says how late
    -- a row could still be picked up.
    billing_cutoff_at         TEXT NOT NULL,

    total_amount_micro_usd    INTEGER NOT NULL DEFAULT 0,
    incomplete_usage_count    INTEGER NOT NULL DEFAULT 0,

    -- `due_at` is NULL for a reconciliation statement, and NULL is the whole
    -- meaning: not "not yet set", not "never". Suspension and the email body
    -- both branch on it being absent.
    due_at                    TEXT,
    paid_at                   TEXT,
    paid_by                   TEXT,
    payment_reference         TEXT,
    payment_note              TEXT,

    -- At-least-once delivery bookkeeping. `email_sent_at` is the only claim
    -- that a message left; the rest is why it might not have.
    email_sent_at             TEXT,
    email_attempts            INTEGER NOT NULL DEFAULT 0,
    email_last_error          TEXT,
    email_next_retry_at       TEXT,

    -- Which instance holds the send lease right now, and until when. An
    -- at-least-once duplicate after a crash between sending and committing is
    -- an accepted trade-off; two instances emailing the same statement
    -- simultaneously is not, and this is what prevents it.
    email_claim_instance      TEXT,
    email_claimed_until       TEXT,

    created_at                TEXT NOT NULL,
    updated_at                TEXT NOT NULL,

    -- No `ON DELETE`: a partner with statements cannot be deleted at all. A
    -- statement is financial history that outlives the useful life of the
    -- usage it summarises, and the product has no "erase a partner's invoices"
    -- operation to offer — deliberately, because that is the operation a
    -- dispute turns on.
    FOREIGN KEY (consumer_id) REFERENCES partners(consumer_id),

    UNIQUE (consumer_id, billing_date),

    CHECK (billing_mode IN ('invoice', 'reconciliation')),
    CHECK (total_amount_micro_usd >= 0),
    CHECK (incomplete_usage_count >= 0),
    CHECK (email_attempts >= 0),
    -- A reconciliation statement has no payment lifecycle at all. Asserting
    -- that here rather than only in the code is what stops a later feature
    -- from marking a settlement statement "unpaid" in the database.
    CHECK (billing_mode = 'invoice' OR (due_at IS NULL AND paid_at IS NULL)),
    -- A paid statement must say when and who; an unpaid one must not claim
    -- to. A payment that is not attributable is not an audit trail.
    CHECK ((paid_at IS NULL AND paid_by IS NULL) OR (paid_at IS NOT NULL AND paid_by IS NOT NULL))
);

-- The two suspension queries and the "which days are missing" catch-up walk
-- are the only reads of this table, and each has its own access path.
CREATE INDEX IF NOT EXISTS idx_daily_statements_consumer_date
    ON daily_statements(consumer_id, billing_date);

-- Suspension: "does this partner have a complete unpaid statement past its due
-- date". Partial, because it only ever wants the unpaid ones, and they are the
-- minority of a table that grows without bound.
CREATE INDEX IF NOT EXISTS idx_daily_statements_overdue
    ON daily_statements(consumer_id, due_at)
    WHERE paid_at IS NULL AND billing_mode = 'invoice';

-- The worker's retry sweep: everything not yet sent, ordered by when it next
-- becomes eligible.
CREATE INDEX IF NOT EXISTS idx_daily_statements_email_pending
    ON daily_statements(email_next_retry_at)
    WHERE email_sent_at IS NULL;

-- The lines of a statement: tokens, the prices they were metered at, and what
-- they cost.
--
-- A line is per (statement, model, price snapshot), NOT per (statement, model).
-- That is the difference between an invoice that can be audited and one that
-- cannot: a partner whose `gpt-4o` price changed at 14:00 yesterday used it at
-- two prices, and merging them into one averaged line would hide both the
-- change and the amount it moved. A statement with one model and one price
-- still has exactly one line, so the common case is not made uglier to protect
-- the rare one.
--
-- Cached and uncached input are both carried: `input_tokens` is what the
-- provider reported, `uncached_input_tokens` is what was charged at the input
-- price, and the difference is what `cached_input_tokens` says. Billing the
-- reported `input_tokens` at the input price as well would charge the cached
-- portion twice.
--
-- `total_cost_micro_usd` is the authoritative amount for the line, because
-- rounding a component independently and re-deriving the sum does not always
-- produce the sum of the parts. The components are kept too, so a statement can
-- be read the way a customer reads it — "why is this line this much".
CREATE TABLE IF NOT EXISTS statement_lines (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    statement_id            INTEGER NOT NULL,

    model                   TEXT NOT NULL,
    -- The three prices, copied onto the line. A statement is immutable, so
    -- these cannot drift from the prices the rows were metered at; they are
    -- what makes a year-old invoice renderable without joining anything.
    input_price_micro_usd_per_million       INTEGER NOT NULL,
    cached_input_price_micro_usd_per_million INTEGER NOT NULL,
    output_price_micro_usd_per_million      INTEGER NOT NULL,

    request_count           INTEGER NOT NULL,
    input_tokens            INTEGER NOT NULL,
    cached_input_tokens     INTEGER NOT NULL,
    uncached_input_tokens   INTEGER NOT NULL,
    output_tokens           INTEGER NOT NULL,

    input_cost_micro_usd        INTEGER NOT NULL,
    cached_input_cost_micro_usd INTEGER NOT NULL,
    output_cost_micro_usd       INTEGER NOT NULL,
    total_cost_micro_usd        INTEGER NOT NULL,

    -- A statement's lines are read with it and never without it.
    FOREIGN KEY (statement_id) REFERENCES daily_statements(id) ON DELETE CASCADE,

    UNIQUE (statement_id, model, input_price_micro_usd_per_million,
            cached_input_price_micro_usd_per_million, output_price_micro_usd_per_million),
    CHECK (request_count >= 0),
    CHECK (input_tokens >= 0),
    CHECK (cached_input_tokens >= 0),
    CHECK (output_tokens >= 0),
    -- The arithmetic invariant of the whole billing module, as a constraint:
    -- a line may not bill more uncached input than the provider reported in
    -- total, and may not claim more cached than that either.
    CHECK (uncached_input_tokens = input_tokens - cached_input_tokens),
    CHECK (uncached_input_tokens >= 0),
    CHECK (input_cost_micro_usd >= 0),
    CHECK (cached_input_cost_micro_usd >= 0),
    CHECK (output_cost_micro_usd >= 0),
    CHECK (total_cost_micro_usd >= 0),
    CHECK (input_price_micro_usd_per_million >= 0),
    CHECK (cached_input_price_micro_usd_per_million >= 0),
    CHECK (output_price_micro_usd_per_million >= 0)
);

CREATE INDEX IF NOT EXISTS idx_statement_lines_statement
    ON statement_lines(statement_id);

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
