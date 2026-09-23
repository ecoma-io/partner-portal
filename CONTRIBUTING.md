# Contributing to Partner Portal

Thank you for being here. This is a single-maintainer project: issues and pull
requests are both welcome, and both go through the same gate.

By contributing you agree that your work is licensed under the
[Apache License 2.0](LICENSE), and that you have the right to grant that license
— see [Ownership of what you contribute](#ownership-of-what-you-contribute).

[`AGENTS.md`](AGENTS.md) carries the working guidance: the seven invariants, the
layout, and the rules a diff is rejected for. Read it before your first change;
this document covers process, and the two overlap only where they must.

## The one question a review starts from

**What happens when this is wrong in the quiet direction?**

Everything in this repository can fail quietly. A dropped record looks like a
quiet hour. A `0` written where the provider said nothing looks like a cheap
request. A query missing `WHERE consumer_id` returns rows that look like the
caller's own. A second rollup applied to the same row looks like a slightly
larger total. None of these raise anything, and all of them are worse than a
crash.

Two consequences, and they shape most reviews:

- **A test that only pins the loud direction is not a test.** If your change
  touches metering, recovery, retention, usage extraction or a dashboard query,
  there must be a case that goes red when the quiet failure happens.
- **Never fix the prose to match a defect.** If the code and the documented
  invariant disagree, say so in the pull request rather than editing the doc.

## The gate

Every non-trivial change follows the organisation's order, with an issue filed
**before** code is written:

1. **Issue** in this repository describing the defect or the change.
2. **Branch** pushed to the remote.
3. **Draft pull request** against the default branch, linking the issue.
4. Tests and docs updated in the same pull request; CI green.
5. Marked **ready for review**, then landed through the **merge queue** — no
   direct pushes to the default branch.

Commits are **signed**. If your setup cannot sign, say so rather than pushing an
unsigned commit.

## Setting up

- **Rust** — `Cargo.toml` declares `rust-version = 1.85` and edition 2024; any
  current stable toolchain works. There is no `rust-toolchain.toml`, so the
  toolchain is yours to manage.
- **Node ≥ 24 and pnpm** — only for the dashboard under `dashboard/`. The binary
  does not need Node at runtime, and a build without a built dashboard is valid
  (it serves a placeholder page and prints a `cargo:warning`).
- **Git hooks are not installed for you.** There is no `package.json` here, so
  nothing runs `lefthook install` on your behalf and a fresh clone has no hooks.
  Run it yourself if you have `lefthook` on `PATH`:

  ```bash
  lefthook install
  ```

  Skipping it is allowed; the same commands run in review, and finding out there
  is a worse trade than running them locally.

```bash
git clone https://github.com/ecoma-io/partner-portal.git
cd partner-portal
cargo build                       # warns if dashboard/dist is missing
pnpm --dir dashboard install && pnpm --dir dashboard build   # optional, embeds the UI
```

## The commands

| Command | What it does |
|---|---|
| `cargo build` | Debug build, `build.rs` embeds `dashboard/dist` if present |
| `cargo build --release` | The shipping artifact: thin LTO, stripped |
| `cargo test --lib` | Unit tests beside the code — the fast loop (~2 s) |
| `cargo test --test integration` | The real binary as a child process, a mock upstream, the ledger read back from SQLite |
| `cargo test --test e2e -- --test-threads=1` | Signals, rolling update, crash recovery — serially, each test spawns processes |
| `cargo fmt --all` | Format. No rustfmt config: the defaults are the style |
| `cargo clippy --all-targets -- -D warnings` | Lint; warnings are errors |
| `cargo bench --bench ledger_write` | Commit-ack p50/p95/p99 against the real writer |
| `pnpm --dir dashboard build` | `vue-tsc -b && vite build` — also the dashboard typecheck |

A change is not ready while any of those is red — including the ones that look
unrelated, such as a `dashboard/dist` that no longer compiles.

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) is the authority on the
full gate list: formatting, `cargo check --locked`, clippy, the tests, the
dashboard's lint/typecheck/build, a Docker smoke test and `cargo audit`. The hooks
above are a fast subset of it, run for you rather than for the branch.

## What the hooks do

| Hook | Commands |
|---|---|
| `pre-commit` | `rustfmt --edition 2024 --check` over staged `*.rs` — a check, never a rewrite |
| `commit-msg` | commitlint over the message — or a shell check of the same rules when commitlint is not installed |
| `pre-push` | `cargo clippy --all-targets -- -D warnings`, `cargo test --lib`, `cargo test --test integration` |

The `pre-commit` hook checks rather than formats on purpose: `cargo fmt` with file
arguments formats the whole crate and `rustfmt` on `src/lib.rs` or `src/main.rs`
walks that file's module tree, so either one would rewrite files you did not
stage. The fix it names is `cargo fmt --all`.

The hooks are deliberately fast: a hook slow enough to notice is a hook people
learn to skip with `--no-verify`. Bypassing a hook during a rebase is occasionally
right; landing a change that way is not.

## Commit messages

Conventional Commits, enforced by [`commitlint.config.mjs`](commitlint.config.mjs):

```
<type>(<scope>): <subject>
```

- **Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`,
  `ci`, `chore`, `revert`.
- **Scopes** name the area and follow the module map: `proxy`, `ledger`, `auth`,
  `dashboard`, `dashboard-ui`, `admin`, `web`, `config`, `telemetry`, `deploy`,
  `docs`, `deps`, `ci`, `release`, `repo`. The scope is optional. `deps` and `ci`
  exist so automation passes the same gate as a human change. A new module lands
  with its scope in the same commit.
- **Breaking changes** add `!` before the colon and a `BREAKING CHANGE:` footer.
- **AI-assisted work** carries one trailer per pull request, on the last commit:
  `Assisted-by: <tool>`, or `Generated-by: <tool>` where the tool produced the
  change rather than helped with it.

Write the pull-request title as a Conventional Commit too — it is the subject a
reader sees first, and the easiest thing to get right before review starts.

## Opening a pull request

1. Branch from the default branch, rebase onto it before opening the pull
   request.
2. Keep it **small and single-purpose**: one behaviour per pull request. A pull
   request that fixes a bug and adds a feature is two pull requests.
3. Update the docs that describe the behaviour you changed —
   `docs/architecture/overview.md`, the ADR that owns the invariant, or
   `config.example.yaml` for a new field. A document that lags the code is a
   defect, not a follow-up.
4. Add the tests, and make sure one of them fails without your change.
5. Open the draft, link the issue, and say what you verified by execution
   (commands, output) versus by reading.
6. Once CI is green, mark it ready. Approved and green means it goes into the
   merge queue immediately — nothing waits to be batched.

## Review expectations

A reviewer will ask about, in roughly this order:

- **The quiet failure.** What breaks silently if this is wrong, and which test
  catches it?
- **The invariants.** Which of the seven does this touch, and does it still hold?
  An invariant that must change needs an ADR in the same pull request.
- **Isolation.** Any new query carries `consumer_id = ?1` from the extractor, and
  any new route takes `Authenticated` as an argument.
- **Compatibility.** A config field must be additive and optional — an operator
  upgrades the binary before the file. A schema change must be safe for a
  database that a previous binary is still writing to during a rolling update.
- **Dependencies.** A new one needs a reason: what it replaces, and why the code
  you would otherwise write is worse.
- **Honesty.** No `unwrap`/`expect` outside tests (except literal parses that say
  why), no fabricated usage, no swallowed error, no doc sentence the code does not
  support.

## Adding a proxied endpoint

The proxy path is a small set of ordered edits, and one of them is easy to miss
because it is in SQL:

1. **`src/ledger/types.rs`** — add the variant to `Endpoint`, its `as_str()`
   (the string that reaches the ledger and the API) and its `from_path()` arm.
2. **`src/ledger/schema.sql`** — extend the `CHECK (endpoint IN (...))`
   constraint. **This is the easy-to-miss step, and it has a compatibility
   consequence:** the table is created with `CREATE TABLE IF NOT EXISTS`, so an
   existing database keeps the *old* constraint and will reject the new value.
   Widening it for a fresh database is not enough; see [Changing the
   schema](#changing-the-schema) for what to do about a live file.
3. **`src/proxy/usage.rs`** — a per-endpoint extractor for the non-streaming body
   and one for the streaming event, returning `Option<Usage>`. Never default to
   zero (ADR 0006).
4. **`src/proxy/handler.rs`** — the `extract_usage` arm, and the model extraction
   if the request body names its model differently. If the endpoint consumes no
   tokens, take the `proxy_unmetered` path instead of metering a zero-usage row.
5. **`src/main.rs`** — register the route on the proxy handler. Nothing else needs
   to change: the body limit, tracing, sensitive headers and CORS are layers over
   the whole router.
6. **Tests** — `from_path`, both usage extractors (including the no-usage case),
   and an integration test that the endpoint is or is not metered.

## Adding a dashboard endpoint

1. **`src/dashboard/api.rs`** — a handler that takes `Authenticated`, resolves its
   window with `TimeWindow`, and filters every query by `consumer_id = ?1` from
   the key. There is no parameter that widens the scope, and adding one is a
   security defect (ADR 0008).
2. **Read from the right source.** Totals and timeseries come from the hourly
   rollup; per-request rows come from the raw ledger. Both are written in one
   transaction so they agree (ADR 0005), but the rollup cannot be sliced finer
   than an hour — round rollup bounds, keep raw bounds exact.
3. **Page, do not dump.** The request list is keyset-paginated
   (`(created_at, id)`, opaque cursor) with `MAX_LIMIT = 200`. A new list is
   paginated the same way.
4. **Errors stay opaque.** `DashboardError` carries a message safe to return; a
   database error must not reach the client with its SQL in it.
5. **Registration** — `create_api_router()` (REST) or `create_dashboard_router()`
   (which adds the SSE route).
6. **Tests** — a scoping test with two consumers, and the window/pagination edge
   cases (unknown range falls back to 24 h, a range past retention is rejected).

## Changing the schema

`src/ledger/schema.sql` is applied on every startup as a batch of
`CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` statements, and it is
idempotent. There is **no migration runner** — the schema is a single embedded
file and there is no migration directory — and `SCHEMA_VERSION` in
`src/ledger/mod.rs` is a version *stamp*, not a step counter.

1. Change `src/ledger/schema.sql` so a **fresh** database has the shape you want.
2. Bump `SCHEMA_VERSION` in `src/ledger/mod.rs`. It is written to
   `ledger_meta.schema_version` at startup and returned by `/version`.
3. **State the compatibility story in the pull request**: an existing database
   does not get your change. A new nullable column or a new table is invisible to
   old code (safe in both directions during a rolling update); a changed `CHECK`,
   a `NOT NULL` addition, or a renamed column is not, and needs a documented path
   — including what an operator running two instances does.
4. Update **both** write paths: `src/ledger/writer.rs` (the live path) and
   `src/ledger/recovery.rs` (the crash path). A column one of them does not know
   about is a difference between a clean shutdown and a crash, which is the
   hardest kind of bug to find.
5. If the change affects retention or the rollup, check
   `src/ledger/retention.rs` and `upsert_hourly` too: they touch every column
   group, and `usage_hourly` derives from `usage_records` in the same
   transaction.

## Changing an invariant

The seven invariants are stated at the top of `src/lib.rs`, each with the ADR that
explains it. Changing one is a product decision, not a refactor, so it lands as a
decision:

1. Add a new ADR under `docs/adr/` (same five sections; see
   [`docs/README.md`](docs/README.md#adding-an-adr)).
2. Mark the superseded ADR with a `Superseded by NNNN` line — never edit its
   decision.
3. Update the crate doc in `src/lib.rs` and the invariant table in
   `AGENTS.md`.
4. Update the tests that enforce it, keeping the quiet-direction case red without
   your change.

## Reporting problems

- **Bugs and feature requests:** this repository's issue tracker. Search first —
  a duplicate costs a maintainer more than a missing report. Describe what you
  observed, and what you cannot reproduce, without guessing at a cause.
- **Anything security-shaped** — an auth bypass, a consumer seeing another
  consumer's traffic, a credential in a log line or an error body, SQL reachable
  from a request: [`SECURITY.md`](SECURITY.md), never a public issue.

## Ownership of what you contribute

You keep the copyright. What you grant is the Apache License 2.0 right to use and
redistribute the work as part of this project — and, per
[Commit messages](#commit-messages), a clear statement of which parts a machine
helped write.
