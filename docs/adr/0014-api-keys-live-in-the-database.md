# 0014 — Partner API keys live in the database, hashed, behind an in-memory snapshot

Status: accepted — supersedes the credential-store half of
[ADR 0008](0008-server-side-consumer-identity.md) (identity is still derived
server-side; it is now derived from a database row rather than a config key) and
relocates the list that [ADR 0012](0012-per-key-model-allow-list.md) gates
traffic with. It narrows ADR 0008's "the config is the credential" clause and
leaves every other decision in both records standing.

Extended by [ADR 0015](0015-daily-postpaid-statements.md), which added a second
table beside `api_keys` — `partners` — and reuses this record's `ManagerOnly`
boundary for the whole commercial surface rather than only the key lifecycle. The
same argument settles both: a credential that could mark a partner's own statement
paid would end that partner's own suspension with one click, so the separation is
between roles and not between screens. The same unconditional refresh interval
that keeps revoked keys from outliving their revocation also keeps a suspended
partner's status and a replaced price list from outliving the change.

## Context

A partner API key was a plaintext line in `config.yaml`
(`keys: [{key, name, consumer_id, allowed_models}]`). The running process held a
config snapshot and matched the presented bearer token against it — an exact
string comparison per request, over a `HashMap` built at load.

Three properties of that design had become the reason for a growing list of
compromises:

1. **The configuration file was the credential store.** Every fixture, dev
   config, deploy config, smoke-test config and README example had to carry keys,
   because a deployment with no keys does not start. The file had to be treated
   as a secret — which it is not, since it also holds the listen tuning and the
   retention window — and `Config` had to redact key values from `Debug`, from
   YAML parse errors and from every recorded reason it logged, because a
   mis-typed file would otherwise print a partner's credential into the log.
2. **Revocation was a file edit.** Rotating a key meant rewriting YAML and
   waiting for the 1 s content-hash watcher, so "revoke this key now" had no
   answer better than "edit the file and hope". It also made the credential store
   the change-propagation channel for a security event.
3. **A key could not be issued without editing the deployment.** There was no
   runtime verb for "add a partner", and `deploy/rolling-update.sh` had to
   regex-parse a key out of the committed YAML to send a verification request.

Meanwhile the deployment already had exactly the store this wants, with exactly
the durability properties it needs: a SQLite ledger, WAL, `synchronous = FULL`,
on a named volume that survives restarts, recreations and rolling updates.

## Decision

**The database is the single source of truth for partner API keys; the
configuration file holds no key, and there is no second path.**

* **A dedicated table** (`api_keys`), created by the existing idempotent
  `schema.sql` on every startup. `SCHEMA_VERSION` rises 4 → 5. `key_hash` is
  `UNIQUE`; `status` is `active` or `revoked` with `revoked_at` agreeing with it
  through a `CHECK`; `allowed_models` is a JSON array; `expires_at` is `NULL` for
  a key that never expires. `key_prefix` is the first 12 characters of the
  plaintext — an operator's way to tell two keys apart in a listing, and
  deliberately **not** a secret.

* **The stored value is a keyed HMAC-SHA256**, `hex(HMAC(PARTNER_PORTAL_API_KEY_SECRET,
  plaintext))`, not a bare digest. The plaintext of a generated key carries 256
  bits of OS entropy, so it is not brute-forceable either way; what keying
  changes is what a stolen `api_keys` table is worth. A bare digest *is* the
  credential — anyone holding the database could authenticate as any partner,
  permanently, with nothing to revoke but the key itself. Keyed, the table is
  inert without a secret that never touches it.

* **Every hash requires `PARTNER_PORTAL_API_KEY_SECRET`**, ≥ 32 bytes, from the
  environment, never from SQLite and never from a config file. A process that
  cannot read it **refuses to start**: an instance that cannot hash a key
  authenticates nobody, and saying so once at startup beats saying
  `invalid api key` per request forever. The minimum is a guard against a typed
  passphrase, not a cryptographic requirement.

* **Authentication reads an in-memory snapshot, never the database.** The
  process holds `Arc<RwLock<ApiKeySnapshot>>` (`parking_lot`, the primitive the
  config snapshot already uses): a `HashMap` keyed by hash-hex. A request's token
  is hashed and looked up — no SQL, no connection, no I/O on the request path.
  Because lookup is a hash-table probe on a value the attacker would have to know
  the secret to produce, no constant-time comparison is needed there; the
  hand-rolled `constant_time_eq` stays where it already served.

* **The snapshot is refreshed on an interval, unconditionally.** A sibling
  instance's revoke would otherwise never be seen, which is the normal case
  during a rolling update rather than an edge one. The obvious change-detection
  design — poll `PRAGMA data_version`, reload when it moves — is *wrong here*,
  and expiry is why: a key's lifetime runs out with no write anywhere, so a
  reload that waits for a change signal never comes for the one transition that
  most needs to happen on time. Every tick therefore reloads
  (`server.api_key_refresh_ms`, default 1000). A local admin mutation commits and
  then refreshes synchronously, so a revoke through *this* process is effective
  on the very next request. A *failed* reload keeps the last good snapshot and
  warns — swapping in an empty set because a poll failed once would take every
  partner offline, and is the "fallback that hides a failure" the repository bans.

* **Keys are managed at `/api/admin/api-keys*`, and the credential is the
  manager password, and nothing else.** A partner key gets `403`
  (`manager_required`); no credential gets `401`. The reasoning is the mirror of
  ADR 0013's: a partner key is scoped to one consumer, and a credential that may
  mint or revoke keys is a path from one partner to another partner's identity.
  The manager password is already refused on `/v1/*`; it is likewise the only
  thing that may issue keys. The surface is `POST` (issue), `GET` (list, and
  one), `PATCH` (name, `allowed_models`), `POST .../rotate`, `POST .../revoke`,
  every response `Cache-Control: no-store`.

* **The plaintext exists in exactly two responses, once each.** `create` and
  `rotate` return `key_secret`; nothing else in the product can. The read type
  (`ApiKeyView`) has no plaintext field and no hash field, built by hand from the
  row rather than derived, so adding a column cannot start publishing it.
  `rotate` inserts the replacement and revokes the predecessor **in one
  transaction**, so a crash leaves the old key valid or the new key valid, never
  both and never neither.

* **A one-shot CLI, `partner-portal keygen`, is the bootstrap.** With no key in
  the YAML, a fresh deployment has zero keys and the only runtime way to create
  one is an API that answers to the manager password — which an operator with the
  config file already has. The gap is convenience and first-run automation, not
  access, so the answer is a subcommand that opens the database, applies the
  schema, inserts one key and prints the plaintext once to stdout. It is a
  provisioning utility, not architecture: the server never invokes it, and
  nothing about serving a request depends on it having run.

* **No dual-read, no dual-write, no fallback.** `Config` has no `keys` field, no
  `KeyConfig`, no `find_key`, no `NoKeys`/`DuplicateKey` error variants and no
  key branch in the hot-reload report. `Config` is `deny_unknown_fields`, so a
  leftover `keys:` block is a **parse error** — the loud break is the design, and
  a deployment that ignores it fails at startup rather than quietly running with
  two credential sources.

## Alternatives considered

* **Keep the YAML, hash the values in it.** Rejected: it moves the redaction
  problem rather than removing it, leaves the file as the revocation channel, and
  leaves the credential in a file that a deployment has to check in or template.
  The store is the thing that is wrong, not the encoding.
* **`argon2`/`bcrypt` per key.** Rejected on two counts. A password hash is
  deliberately slow because a password is guessable and low-entropy; a generated
  key is 256 random bits, so the work factor buys nothing. And it is slow *per
  authentication* — the cost lands on the request path, for every inference
  request, where the design's whole point is that auth costs one hash-table probe.
  (A salted KDF would also break the plain `HashMap` lookup unless the salt were
  addressed by a prefix, reintroducing a second lookup layer.)
* **A bare SHA-256, no key.** Rejected — see the Decision: the stored digest
  becomes the credential, so database read access becomes permanent impersonation
  of every partner.
* **Store the HMAC secret in SQLite.** Rejected outright: a single file would
  then be both the digests and the key that makes them useful, which is the same
  as storing the keys. The secret belongs to the deployment's environment, where
  it is rotated and backed up alongside the database (see Consequences).
* **Query `api_keys` per request** (with a short-lived cache, or on
  `data_version`). Rejected: it puts SQLite on the request path of every
  inference call, makes authentication fail when the ledger is contended — which
  is exactly what `tests/fault/writer_failure.rs` and
  `tests/fault/queue_saturation.rs` simulate — and gains nothing, because the key
  set is dozens of rows that change on human timescales.
* **`arc_swap` for the snapshot.** Rejected: one more dependency for a
  read-mostly `RwLock` whose reads are nanoseconds against the hash lookup they
  precede, when the repository already proves the `Arc<RwLock<…>>` swap works in
  `AppState.config`.
* **`data_version` change detection with a periodic expiry sweep.** Rejected as
  two mechanisms where one suffices; the interval reload covers both the sibling's
  revoke and the passive expiry, and its cost is one indexed `SELECT` per tick.
* **An env-seeded bootstrap key, or auto-generating a key at first startup.**
  Rejected: it makes credential creation implicit and reintroduces "a secret in
  the environment is the product's key store". The CLI is explicit, runs once,
  and prints the key to a human.
* **Keeping the YAML keys as a fallback during a transition.** Rejected by the
  brief and by design: two sources of truth for a credential is a
  two-sources-of-truth bug with a security shape, and the deployment can move its
  existing plaintexts across with `keygen --plaintext` in one command per key.
* **A separate `api_keys` database file.** Rejected: the ledger's volume, WAL
  mode, `busy_timeout`, backup path and crash recovery are already the
  deployment's durable state. A second file is a second thing to back up, a
  second thing to lock, and a second thing that can be on a volume that survives
  a container while the other does not.

## Consequences

* **The configuration file stops being a secret store.** `config.yaml` holds the
  upstream credential and the manager password, which are its own; it holds no
  partner key. `.gitignore`'s rationale, `SECURITY.md`'s threat table, the
  README's configuration table and every fixture change accordingly.
* **An upgrade from schema 4 arrives with zero keys.** `api_keys` is created
  empty, so every partner request is a `401` until an operator provisions keys —
  and the process logs a warning at startup rather than refusing to start, so a
  deployment is never bricked by the migration. Existing plaintexts keep working
  if they are registered with `keygen --plaintext` before the new instances take
  traffic; the credential itself never has to reach a partner again.
* **A revoke reaches a sibling within `api_key_refresh_ms`.** That interval is
  the propagation bound in both directions — the delay before a sibling's revoke
  lands, and the delay before an expiry does. It is stated here rather than
  implied, and it is configurable because a single-instance deployment has
  nothing to wait for.
* **The HMAC secret cannot be rotated in place.** Every stored hash was computed
  with it and there is no plaintext to re-hash, so a new secret invalidates every
  issued key at once; rotating it means re-issuing. It is a deployment secret
  with the same custody as the upstream key, plus one extra rule: a backup of the
  ledger is only usable alongside the secret it was written under.
* **A key cannot be recovered, only replaced.** `keygen` and `rotate` are the
  only sources of a plaintext, and both emit it once. `rotate` preserves the
  consumer and issues a new id, so the lifecycle stays auditable — the old row is
  revoked, not overwritten.
* **`key_prefix` is published.** It is the first 12 characters of the plaintext
  and appears in every listing and in the startup log's prefix count. It carries
  no entropy beyond what it takes to be non-secret by construction; a plaintext
  shorter than 12 characters would be stored in full, which is why generated keys
  are never shorter and why the fixtures are longer than the prefix.
* **The request path is untouched.** Invariants 1–6 are unaffected: no metering
  behaviour depends on where a key came from. Invariant 7 (dashboard data is
  key-scoped) is strengthened, if anything — identity now comes from a row that
  no request field can reach.
* **`/v1/models` and the model gate are unchanged** (ADR 0012): the allow-list
  still rides the credential, is still strict by default, still refuses with
  `404 model_not_found` before the upstream and before a ledger row. Only its
  storage moved, and `keygen` deliberately has no `--all-models`.
* **Deployment ordering matters once.** During the rollout that introduces this,
  a new binary reads the database and an old binary reads the YAML, so the keys
  must exist in the database before an instance that needs them takes traffic.
  The rolling-update script provisions before it switches rather than assuming.

## Evidence

* `src/ledger/schema.sql` — `api_keys`, its `CHECK`s, `UNIQUE (key_hash)`,
  `idx_api_keys_active`; `src/ledger/mod.rs` — `SCHEMA_VERSION = 5` and its
  history line, `init_schema`.
* `src/apikeys/mod.rs` — `SECRET_ENV`, `MIN_SECRET_LEN`, `KEY_PREFIX`,
  `KEY_PREFIX_LEN`, `derive_key_hash`, `generate_plaintext`, `load_secret`;
  `src/crypto.rs` — `hmac_sha256`, `constant_time_eq`, `base64url_encode`.
* `src/apikeys/store.rs` — `ApiKeyStore` (`authenticate`, `refresh`, `create`,
  `create_with_plaintext`, `get`, `list`, `update`, `revoke`, `rotate`),
  `ApiKeyRow`, `ApiKeyAuth`, `ApiKeySnapshot` (`from_rows`, `get`); every
  mutation inside `pool.write` and one `Transaction` through the private
  `mutate`, which refreshes this instance's snapshot before returning.
* `src/apikeys/refresher.rs` — the periodic unconditional reload, the
  keep-the-last-good-snapshot rule, `server.api_key_refresh_ms`.
* `src/auth/middleware.rs` — `state.api_keys.authenticate(token)`, the manager
  branch still on the config snapshot.
* `src/admin/keys.rs` — `create_key_router`, `ManagerOnly`, `ApiKeyView` (no
  plaintext, no hash), `IssuedKeyView.key_secret`, the rotate transaction, the
  `no-store` headers.
* `src/keygen.rs` — the `keygen` subcommand, its `--plaintext` migration path and
  its stdout/stderr split.
* `src/config/types.rs`, `src/config/loader.rs`, `src/config/hot_reload.rs` — the
  absence of `keys`, `KeyConfig`, `find_key`, `NoKeys` and `DuplicateKey`.
* `config.example.yaml`, `deploy/config/*.yaml`, `.gitignore` — the pointer to
  the admin API and the statement that no key is in any configuration file.
* Tests: the unit tests in `src/apikeys/mod.rs`, `store.rs`, `refresher.rs` and
  `src/keygen.rs`; `tests/integration/api_key_admin.rs` and
  `tests/e2e/api_key_refresh.rs` (the lifecycle, rotation, restart persistence
  and the cross-instance refresh bound); `tests/integration/auth.rs` (revocation
  through the admin API with the pid unchanged);
  `tests/fault/api_key_snapshot.rs` (a revoke while the writer is held);
  `tests/integration/model_allow_list.rs` (the gate, unchanged); and
  `tests/fault/writer_failure.rs` / `tests/fault/queue_saturation.rs`, which
  keep passing unchanged only because authentication never consults SQLite.
