# 0015 — Daily postpaid statements from frozen usage and price snapshots

Status: accepted — extends ADR 0006 (usage is never fabricated) from the request
path to a money consequence, and extends ADR 0008 (server-side identity) by
making `consumer_id` a commercial boundary. It does not supersede any earlier
decision; where the two touch, this record is the one that owns the behaviour.

## Context

The product metered every request, refused usage it could not verify, and showed
an operator the token counts — and stopped there. `consumer_id` was an isolation
boundary and nothing else: two keys could share it, there was no record of who a
consumer *was*, and there was no way to answer "what does this partner owe".

Turning usage into money is where a metering product stops being a metering
product, and the shapes that are easy to get wrong are all shapes this repository
has already refused elsewhere:

1. **The price used at billing time is not the price that was in force at
   request time.** An operator edits a price list, and a bill computed from
   *today's* list charges yesterday's traffic at today's rate. Retroactive
   repricing is invisible in the code and impossible to argue about in a
   dispute.
2. **A day is not a calendar day.** A request accepted at 23:59:59 is metered
   before the upstream is contacted (ADR 0003) and finalised after it. Closing
   the day at midnight drops those rows, and the partner sees an under-bill
   exactly at the boundary they check first.
3. **"We do not know what this cost" and "this was free" are the same value in
   most schemas.** ADR 0006 refuses to write a `0` where the provider said
   nothing. Give that `NULL` a money consequence and it becomes the failure
   this product cannot have, because the partner pays the difference between
   what was billed and what it cost.
4. **A statement that recomputes from live data changes after it is sent.** The
   usage table is pruned by retention (ADR 0002's deployment). A bill that moves
   when a sweep runs is not a bill.
5. **Suspension is a policy with a clock on it**, and a clock has two failure
   modes: a statement that suspends nobody, and a partner suspended for a
   statement that was never owed. Both are decided by comparing timestamps, and
   both must be reproducible in an audit.
6. **Email is the delivery mechanism for a financial document.** A send that
   fails halfway is the difference between a partner who has seen an invoice and
   one who has not, and the retry must not be the thing that duplicates it.

## Decision

**Each partner receives at most one immutable statement per billing day, priced
from snapshots frozen onto the usage row when the request was accepted. A
partner on `invoice` terms is emailed it and is suspended while a complete
statement is past its due date; a partner on `reconciliation` terms is issued it
as a settlement record and owes nothing.**

* **`partner` is the commercial unit; `consumer_id` remains the identity and
  isolation boundary, and the ledger's usage rows are not refactored to carry a
  `partner_id`.** They are the same value today, and merging the two would put a
  commercial edit on the path of every usage query and make "who is billed" a
  question the request path can answer. `partners.consumer_id` is a primary key
  with a foreign key from `daily_statements`, so the commercial record cannot
  outlive the identity it bills.

* **Prices are exact fixed-point micro-USD per million tokens, and the price is
  frozen onto the usage row at accept time.** `usage_records` gains
  `input_price_snapshot`, `cached_input_price_snapshot` and
  `output_price_snapshot`; they are written by the same statement that writes
  the tokens, in the same batch, or not at all. `PricePerMillion::parse` builds
  the value digit by digit rather than through `f64`, because the reason the unit
  is micro-dollars is that the arithmetic is exact, and routing a configuration
  value through a float reintroduces the bug one layer below the one that was
  fixed. A statement then joins nothing: it reads the snapshots off its own
  lines. A price change mid-day produces two groups for one model and the
  statement keeps both, because the operator changed the price for a reason and
  the statement is how anyone finds out what it cost.

* **`NULL` is never `0`, and a not-billable request contributes a count and no
  money.** A request whose usage the provider did not report, or whose model has
  no price, is counted in `incomplete_usage_count` and contributes no cost. The
  statement is still issued and still shows the count next to the total. This is
  invariant 3 unchanged, promoted from a correctness property to a contractual
  one: the `CHECK`s in `schema.sql` are the same ones, and a statement whose
  `incomplete_usage_count > 0` is visibly not a complete bill.

* **A statement is issued once per day, by a scheduler, after a close delay.**
  `billing.timezone_offset_minutes` names the day — a fixed offset rather than a
  zone name, so a tz database update cannot move a statement by an hour — and
  `billing.close_delay_minutes` (default 5) is the window after the day ends in
  which the last requests of that day are still finalising.
  `billing.scheduler_interval_secs` (default 30) is how often the process
  *notices*, not the deadline; the deadline is the close delay. Sweeps are
  idempotent and a closed day is never reopened, so a shorter interval costs a
  query, not a bill. `UNIQUE (consumer_id, billing_date)` is what makes
  "at most one" true: a duplicate invoice is not a failure the code can absorb.
  `BillingDay::days_to_close` walks the gap after a restart, so an instance that
  was down for a week issues the days it missed rather than skipping them.

* **`invoice` and `reconciliation` are different products, not a flag.**
  `invoice` carries an obligation: a `due_at`, an email, and suspension. A
  reconciliation statement has **no** `due_at` — `NULL` is the whole meaning, not
  "not yet set" — is never payable, and the database says so in a `CHECK`
  (`billing_mode = 'invoice' OR (due_at IS NULL AND paid_at IS NULL)`) so a
  later feature cannot mark a settlement record "unpaid". An operator can create
  a partner in either mode; nothing else in the product reads the field.

* **Suspension is derived at read time from the same function, never stored.**
  `status_for(…)` takes the statements, the clock and the partner's mode and
  answers `active` or `suspended` with the reason. It is used by the request path
  (which refuses a suspended partner before metering), by `/api/billing/status`
  (which tells a partner why), and by the manager's summary (which counts) — so
  those three cannot disagree, and a payment clears suspension with no write
  anywhere. A suspended partner's status is available to a partner key, so the
  dashboard never re-derives it and never shows a partner "suspended" while the
  proxy is still serving them.

* **The request path does no billing work.** Prices and service status are
  resolved from an in-memory `PartnerRuntimeConfig` snapshot, refreshed on the
  same unconditional interval as the key snapshot (ADR 0014) and synchronously
  after any admin mutation. No ledger query, no due-date arithmetic, no statement
  read on the path of an inference request. A partner with no configured model
  may call **nothing**, which is the only safe reading of "not configured" and
  the same strict default as ADR 0012's allow-list.

* **Money crosses no boundary in this product.** There is no Stripe, no wallet,
  no subscription, no coupon, no tax engine, no automatic charging, and no
  payment-provider abstraction. A payment is an operator recording that money
  moved elsewhere, on one endpoint, against an invoice statement that is
  outstanding. The endpoint is idempotent: a second `mark-paid` is *answered*,
  not refused, because the caller's intent is already satisfied and a 409 would
  make an operator wonder whether the first one worked. Marking a reconciliation
  statement paid is `409 statement_not_payable` — the server is the authority, and
  the dashboard's own gate on `invoice && outstanding && !paid_at` is a
  convenience, not the protection.

* **A statement is sent by an at-least-once email path that records what
  happened.** `email_sent_at` is the only claim that a message left; the rest
  (`email_attempts`, `email_last_error`, `email_next_retry_at`) is why it might
  not have. A failed send is retried on its own schedule and never blocks the
  statement from being durable. The message body carries the statement's own
  figures and **no credential, no API key, no upstream key, no request body**.

* **The SMTP credential is a deployment environment variable and nothing else.**
  `PARTNER_PORTAL_SMTP_USERNAME` and `PARTNER_PORTAL_SMTP_PASSWORD`, read into
  memory at startup, never written to SQLite and never present in YAML. The pair
  is read as a pair: neither variable is an anonymous relay and not an error,
  and exactly one is a warning plus a skipped send rather than a startup failure
  — half a credential cannot authenticate, and a proxy that refuses to boot over
  a mis-set mail variable is a worse outcome than a statement that was written
  and not sent, which the manager's statement detail already reports.
  `EmailConfig` has no password field at all, so `Config` cannot be a place one
  ends up.

* **The partner surface is read-only and the manager surface is the only write
  surface**, both behind the same `ManagerOnly` boundary as ADR 0014's key
  lifecycle. A partner key gets `403 manager_required` on every admin route, and
  the dashboard's partner page has no payment control to omit later. Identity and
  scope are still resolved server-side (ADR 0008): a `consumers=` parameter is
  read for a manager and ignored for a partner key, and a statement outside the
  caller's scope is a 404.

## Alternatives considered

* **Price the statement from the partner's current price list.** Rejected: it
  makes a price change retroactive by default, and the retroactive change is
  invisible in the code. The snapshot is the fix, and it is cheap — three
  integers on a row that is already being written.
* **Store the price as a string or as dollars on the usage row.** Rejected: the
  request path and the statement arithmetic would both have to parse it back, and
  a parse is where exactness is lost. The unit is an integer; the decimal string
  exists only at the wire and config edges.
* **A monthly statement, or a configurable period.** Rejected: a day is the
  smallest period that is still a business object, it keeps the reconciliation
  window short, and it is what makes a dispute about one request tractable. The
  offset and the close delay are the two knobs that matter; the period length is
  not one.
* **Close the day the moment it ends.** Rejected: the last requests of the day
  are still finalising, and the resulting under-bill is systematic and lands
  exactly where a customer looks first. The close delay is the window in which
  they land, and it is why `billing_cutoff_at` is recorded on the statement and
  is never `period_end`.
* **Charge the incomplete requests at zero and say nothing.** Rejected: it is
  invariant 3 with a price attached, and the partner would read a total that is
  wrong by an unknown amount. The count beside the total is the honest form.
* **A statement per request.** Rejected: it turns a dispute about a bill into a
  dispute about thousands of rows, and it is not a document anyone can act on.
* **A statement per partner per month, with a `statement_lines` table recomputed
  on read.** Rejected: recomputation is what makes a bill move after it is sent.
* **Recompute totals from live usage on the dashboard.** Rejected for the same
  reason, and because retention prunes usage — a lifetime total that shrank when
  a sweep ran is worse than no total. The manager's summary reads the statements
  that exist, never the usage underneath them.
* **Store the suspension state on the partner row.** Rejected: a stored
  `suspended` flag is a second source of truth that a payment has to clear and a
  crash can leave stale, and it is a boolean where the real answer is a
  comparison of a clock against a due date. Derived at read time means the
  answer is the same in all three places that use it, with no write anywhere.
* **Suspend on any overdue statement, including an incomplete one.** Rejected:
  an incomplete statement has no amount, so suspending on it enforces a payment
  obligation the product cannot state. Suspension requires a complete statement.
* **A webhook or a payment-provider integration for collection.** Rejected: it is
  a second system with credentials of its own, on the path where a mistake charges
  a customer, for a product whose stated model is postpaid with a human in the
  loop. It is a different product.
* **Auto-charging on a stored card.** Rejected on the same grounds, harder.
* **SMTP password in the config file, next to the upstream key.** Rejected: the
  config is not a secret store (ADR 0014) and the password is a second thing to
  redaction-guard in every `Debug` and every parse error.
* **Store the rendered statement body in SQLite so the email is reproducible.**
  Rejected: the statement *is* the document, and a stored rendering is a second
  copy that can disagree with it.
* **A separate billing database file.** Rejected for the same reason as ADR
  0014's: the statements' whole value is that they are in the same durable,
  backed-up, WAL'd file as the usage they summarise, with the same foreign key
  that makes a billed partner undeletable.

## Consequences

* **The ledger grows by three integer columns and four tables**
  (`partners`, `partner_models`, `daily_statements`, `statement_lines`).
  `SCHEMA_VERSION` rises 5 → 6. The schema is applied on every startup and is
  idempotent, so an existing database gains the tables and the columns empty and
  a new binary serves traffic while a previous one is still writing: a request
  accepted by an old binary has `NULL` price snapshots, and a statement built
  from it counts it as incomplete rather than billing it at an assumed price.
  The alternative — refusing to start on a version the file is not at — would
  make the rolling update, which is a supported operation, fail.
* **Statements are never deleted by retention.** Usage is pruned; a statement
  outlives it. This is the reason a statement carries its own token counts and
  prices, and the reason a customer's invoice does not change when a sweep runs.
* **A partner's price list is what gates their traffic**, and it is stricter than
  it looks: a model with no price is a request that cannot be metered, so
  `PUT /api/admin/partners/{id}/models` takes the complete list and applies it
  atomically. A half-applied price list is a partner whose requests are refused
  for reasons they cannot see. A partner may be created with no models at all,
  which means "calls nothing" — the only safe reading of "not configured".
* **A partner with a statement cannot be deleted**, refused by the API with the
  reason in the message. Their billing history is a financial record, and the
  product deliberately has no "erase a partner's invoices" operation, because
  that is the operation a dispute turns on.
* **The manager password is the only credential that can bill, mint keys, or
  administer a partner.** A credential that could mark a partner's own statement
  paid would end that partner's own suspension with one click, so the separation
  is between roles and not merely between screens.
* **An operator cannot see a partner's price list changing under a live
  statement**, because a statement is immutable. A price edit affects requests
  accepted after the edit and nothing else, and the admin mutations refresh the
  snapshot synchronously so the next request sees it.
* **The dashboard's money is the server's money.** Every figure on a statement,
  a summary or a partner card is a string the server formatted; nothing in the
  SPA parses a price into a JavaScript number or adds up a line. The one integer
  the browser validates is `payment_terms_minutes`, which the server checks as a
  plain integer.
* **Statement email is a delivery channel, not a receipt.** Sending is
  best-effort and at-least-once; the statement is durable whether or not a
  message left. An operator's "did they get it" question is answered by the
  manager's statement detail, which shows `email_sent_at`, the attempt count and
  the last error — not by resending.
* **The billing scheduler is a background task with a stop signal**, and it is
  joined during the same drain as the metering writer. It writes through the
  same single-writer pool, so it cannot contend with the request path for the
  database in a way ADR 0002's design forbids.
* **The request path is unchanged in shape.** Invariants 1–7 all still hold:
  a suspended partner is refused before metering, metering is never dropped, a
  `0` is still never written where the provider said nothing, the identity is
  still the credential's, and dashboard data is still key-scoped.

## Evidence

* `src/billing/mod.rs` — the module contract: what this is, what it is not, and
  the four rules (nothing is invented, the request path does no billing work, a
  statement is immutable, at most one per day);
  `DEFAULT_PAYMENT_TERMS_MINUTES`.
* `src/billing/pricing.rs` — `MicroUsd`, `PricePerMillion`, `TOKENS_PER_PRICE_UNIT`,
  `PRICE_DECIMAL_PLACES`, the digit-by-digit `parse` (and the shapes it refuses:
  a sign, an exponent, a trailing `.`, whitespace), `PricingSnapshot`, `LineCost`,
  `price_line`, `billable`, `needs_cached_count`.
* `src/billing/period.rs` — `BillingTimezone`, `BillingDay`, `BillingPeriod`,
  `cutoff_for`, `closable_through`, `days_to_close`, `oldest_closeable`,
  `catchup_truncated`.
* `src/billing/statements.rs` — `Generator::statement_for_day`, `line_for`,
  `IncompleteReason`, and the grouping that keeps a mid-day price change as two
  lines rather than averaging them.
* `src/billing/worker.rs` — `BillingWorker::spawn` / `run_once`, `Tick`,
  `interval`, `sends_email`; the idempotent sweep and the restart catch-up.
* `src/billing/email.rs` — the at-least-once send, the retry schedule, and the
  body that carries figures and no credential.
* `src/billing/store.rs` — `BillingStore` (`create_partner`, `update_partner`,
  `replace_models`, `total_billed`, `count_statements`, `models`, `get_partner`,
  `list_partners`, `delete_partner`, `mark_paid`, `get_statement_by_id`),
  `NewPartner::validated`, `validate_draft`, `PartnerPatch`, `BillingError`.
* `src/billing/partner.rs` — `BillingMode` (`owes_payment`), `Partner`,
  `ModelPrice`, `PartnerRuntimeConfig`, `PartnerSnapshot`.
* `src/billing/status.rs` — `status_for`, `ServiceStatus`, `SuspensionReason`;
  the one function the request path, the partner's status endpoint and the
  manager's summary all call.
* `src/billing/api.rs` — `create_billing_router`, `StatementView::for_partner`
  (which omits the operational fields), `StatementList`, `ServiceStatusView`.
* `src/admin/billing.rs` — `create_manager_billing_router`, `BillingSummary`,
  `MarkPaidRequest`, the `409 statement_not_payable` guard, and the idempotent
  second `mark-paid`.
* `src/admin/partners.rs` — `create_partner_router`, `PartnerView`,
  `ModelPriceView` (prices as decimal *strings*), `CreatePartnerRequest`,
  `UpdatePartnerRequest`, `ReplaceModelsRequest`, `parse_models`, `parse_price`
  (which delegates to `PricePerMillion::parse` rather than reimplementing it),
  and `refresh_snapshot` after every mutation.
* `src/admin/common.rs` — `ManagerOnly`, `AdminError::into_response` and its
  `not_found` / `partner_exists` / `invalid_request` mapping.
* `src/config/smtp.rs` — `SMTP_USERNAME_ENV`, `SMTP_PASSWORD_ENV`, the
  set-together rule, and the hand-written `Debug` that renders no credential.
* `src/config/types.rs` — `BillingConfig` (`timezone_offset_minutes`,
  `close_delay_minutes`, `scheduler_interval_ms`, `email`),
  `src/ledger/mod.rs` — `SCHEMA_VERSION = 6`.
* `src/ledger/schema.sql` — the three price-snapshot columns on `usage_records`;
  `partners`, `partner_models`, `daily_statements` (its
  `UNIQUE (consumer_id, billing_date)` and the `CHECK`s that make a
  reconciliation statement unpayable and a paid statement attributable),
  `statement_lines` (its own arithmetic `CHECK`s and the
  `UNIQUE (statement_id, model, …prices)` that matches the generator's grouping).
* `src/main.rs` — the billing scheduler is spawned and stopped inside the
  existing drain sequence; `src/ledger/retention.rs` — usage is pruned, and
  statements are not touched by it.
* `config.example.yaml`, `dev/partner-portal.dev.yaml`, `deploy/config/*.yaml` —
  the `billing:` block, with no SMTP password in any of them.
* Tests: the unit tests in `src/billing/pricing.rs`, `period.rs`, `statements.rs`,
  `worker.rs`, `email.rs`, `store.rs`, `status.rs` and `admin/partners.rs`;
  `tests/integration/billing.rs` (a day's statement, a mid-day price change kept
  as two lines, an incomplete request counted and not billed, the immutable
  statement read back); `tests/integration/billing_admin.rs` (the partner
  lifecycle, the atomic price replacement, the 409 on a reconciliation
  statement, the idempotent payment, the delete refusal);
  `tests/e2e/billing_worker.rs` (a day closed across a restart, and the
  catch-up after a gap); `tests/fault/billing_suspension.rs` (a suspended
  partner refused before metering, and resumed with no write to the partner row).
