//! The `api_keys` repository: the only code that reads or writes that table.
//!
//! # One write boundary, like the ledger
//!
//! Every mutation goes through [`LedgerPool::write`], which locks the single
//! `Arc<Mutex<Connection>>` the metering writer uses. That is what keeps
//! "one controlled SQLite write serialization boundary per process" true once a
//! second writer exists: key CRUD and usage metering contend for the same lock
//! rather than running two writers against one WAL file.
//!
//! Key CRUD deliberately does **not** go through the metering queue. The queue
//! exists to apply backpressure to request traffic; an administrator issuing a
//! key is not request traffic, and routing it through would make a key creation
//! fail because the ledger was busy.
//!
//! # Reads
//!
//! Reads go through [`LedgerPool::read`], which opens a fresh connection — the
//! same thing the dashboard does. Nothing on the *authentication* path calls
//! any of these: authentication reads an in-memory [`ApiKeySnapshot`].

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use parking_lot::RwLock;
use rusqlite::{Connection, OptionalExtension, Row, params};
use time::OffsetDateTime;

use crate::apikeys::{derive_key_hash, generate_plaintext, key_prefix_of};
use crate::billing::partner::{BillingMode, PartnerRuntimeConfig, PartnerSnapshot};
use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
use crate::billing::status::{OverdueRow, ServiceStatus, status_for};
use crate::ledger::pool::LedgerPool;
use crate::ledger::timefmt;

/// What a key row looks like to an operator.
///
/// Deliberately has no `key_hash` and no plaintext field: nothing that flows
/// into an HTTP response carries either. The `Debug` derive is safe for the same
/// reason, so this type can be logged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyRow {
    pub id: i64,
    pub name: String,
    pub consumer_id: String,
    pub key_prefix: String,
    pub status: KeyStatus,
    pub created_at: String,
    pub updated_at: String,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

/// Lifecycle state of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyStatus {
    Active,
    Revoked,
}

impl KeyStatus {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "active" => Some(KeyStatus::Active),
            "revoked" => Some(KeyStatus::Revoked),
            _ => None,
        }
    }
}

// The snapshot type a request authenticates against used to live here, as
// `ApiKeyAuth` and `ApiKeySnapshot`. It moved to
// [`crate::billing::partner::PartnerRuntimeConfig`] and
// [`crate::billing::partner::PartnerSnapshot`] when model capability moved out
// of `api_keys.allowed_models` and into `partner_models` (ADR 0015): the thing
// a request resolves is no longer "which key is this" but "which partner is
// this, what may they call, what does it cost, and are they currently served".
// Those are one object now, because they are one lookup.

/// A partner already has a usable key, and the database refused a second one.
///
/// The rule is "one partner, one active key" (docs/adr/0015) and it is enforced
/// by a partial unique index rather than by a check in this module — a
/// check-then-insert is a race between two instances serving two concurrent
/// creates, and the thing that must never happen is a partner ending up with
/// two live credentials. Naming it as its own error is what lets the admin API
/// answer `409` with an explanation instead of leaking a SQLite constraint
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartnerKeyExists {
    pub consumer_id: String,
}

impl std::fmt::Display for PartnerKeyExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "consumer {:?} already has an active api key; rotate it instead of \
             creating a second one",
            self.consumer_id
        )
    }
}

/// A failure in the key store.
#[derive(Debug)]
pub enum ApiKeyError {
    /// SQLite refused the operation.
    Database(rusqlite::Error),
    /// No key has that id.
    NotFound(i64),
    /// The operation needs a key that is still usable, and this one is not —
    /// rotating an already-revoked key, for instance.
    NotActive(i64),
    /// This consumer already has an active key.
    AlreadyActive(PartnerKeyExists),
    /// A field was empty or malformed before it reached SQL.
    Invalid(String),
}

impl std::fmt::Display for ApiKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ApiKeyError::Database(e) => write!(f, "api key database error: {e}"),
            ApiKeyError::NotFound(id) => write!(f, "no api key with id {id}"),
            ApiKeyError::NotActive(id) => {
                write!(f, "api key {id} is revoked and cannot be changed")
            }
            // `PartnerKeyExists` carries the whole sentence, including the
            // remedy: it is rendered on its own in an HTTP body and as the
            // `Display` of this variant, and prefixing it here produced
            // "consumer consumer ..." in one of the two.
            ApiKeyError::AlreadyActive(conflict) => write!(f, "{conflict}"),
            ApiKeyError::Invalid(msg) => write!(f, "invalid api key: {msg}"),
        }
    }
}

impl std::error::Error for ApiKeyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ApiKeyError::Database(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for ApiKeyError {
    fn from(e: rusqlite::Error) -> Self {
        ApiKeyError::Database(e)
    }
}

type Result<T> = std::result::Result<T, ApiKeyError>;

/// `SQLITE_CONSTRAINT_UNIQUE`, the extended code behind every `UNIQUE`
/// violation — including a partial unique index, which reports it as a plain
/// unique violation rather than as anything index-shaped.
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;

/// Whether a SQLite error is a `UNIQUE` violation.
fn unique_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(e, _) if e.extended_code == SQLITE_CONSTRAINT_UNIQUE
    )
}

/// Whether this consumer already has a key that would authenticate.
///
/// Read *after* a unique violation, so it is a disambiguation of a failure
/// that has already happened rather than a guard against one: at this point
/// the answer cannot change the outcome, only which error is reported.
fn active_key_exists(conn: &Connection, consumer_id: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM api_keys WHERE consumer_id = ?1 AND status = 'active' LIMIT 1",
        [consumer_id],
        |_| Ok(()),
    )
    .is_ok()
}

/// The key store: repository plus the snapshot authentication reads.
pub struct ApiKeyStore {
    pool: Arc<LedgerPool>,
    secret: Arc<Vec<u8>>,
    snapshot: Arc<RwLock<PartnerSnapshot>>,
}

impl ApiKeyStore {
    pub fn new(pool: Arc<LedgerPool>, secret: Vec<u8>) -> Self {
        Self {
            pool,
            secret: Arc::new(secret),
            snapshot: Arc::new(RwLock::new(PartnerSnapshot::default())),
        }
    }

    /// The live snapshot, for the authentication extractor.
    pub fn snapshot(&self) -> Arc<RwLock<PartnerSnapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// The ledger this store writes to.
    ///
    /// Shared rather than owned per repository: `partner_models` is written by
    /// the billing store and read by this one's snapshot, and two handles to
    /// one pool is what keeps them serialising against each other.
    pub fn pool(&self) -> &Arc<LedgerPool> {
        &self.pool
    }

    /// Authenticate a plaintext key. No SQLite, no I/O — a hash and a lookup.
    ///
    /// Returns the whole partner configuration, not just an identity: the model
    /// allow-list, each model's price and the service status all come from this
    /// one object, so a request cannot be authenticated against one revision of
    /// the configuration and priced against another.
    pub fn authenticate(&self, plaintext: &str) -> Option<Arc<PartnerRuntimeConfig>> {
        let hash = derive_key_hash(&self.secret, plaintext);
        self.snapshot.read().get(&hash).cloned()
    }

    /// How many keys will currently authenticate.
    pub fn active_count(&self) -> usize {
        self.snapshot.read().len()
    }

    /// Reload the snapshot from the database and swap it in.
    ///
    /// Called at startup, by every mutation (see [`Self::mutate`]) and by the
    /// refresher's periodic tick, which is how a *sibling* instance's commits
    /// arrive. Failures leave the previous snapshot in place — the caller
    /// decides whether that is fatal, and at startup it is.
    pub fn refresh(&self) -> Result<usize> {
        let rows = self.load_active()?;
        let count = rows.len();

        // Reported as *transitions*, not as states. This runs once a second, so
        // a line per rebuild saying "this partner is suspended" would be ten
        // thousand lines a day saying nothing happened; a line saying "this
        // partner just lost service, for statement 12, $1.25, due then" is the
        // one an operator acts on. The comparison is by key hash, so a partner
        // whose key was rotated reads as a new credential rather than a status
        // change — and a revoked key disappearing is not a resume, which is why
        // only hashes present in both snapshots are compared.
        let next = PartnerSnapshot::from_rows(rows);
        {
            // Scoped: the write guard below must not be taken while this read
            // guard is alive, or the refresh deadlocks against itself.
            let previous = self.snapshot.read();
            log_service_transitions(&previous, &next);
        }
        *self.snapshot.write() = next;
        Ok(count)
    }

    /// Every key that would authenticate right now, with the partner it belongs
    /// to resolved whole.
    ///
    /// # Why this is one read transaction
    ///
    /// It runs four queries — active keys, partners, configured models, the
    /// oldest overdue invoice — and the snapshot they produce has to be a
    /// picture of *one* moment. Without a transaction each statement takes its
    /// own read snapshot, so a `partner_models` replace committing between the
    /// keys query and the models query would publish a partner whose model list
    /// is neither the old one nor the new one. The concurrency this guards is
    /// not hypothetical: replacing a partner's models is a single write and the
    /// refresher runs every second.
    ///
    /// `BEGIN DEFERRED` on a WAL connection is enough — the read snapshot is
    /// taken at the first statement and held until commit — and it takes no
    /// write lock, so it cannot block the metering writer.
    ///
    /// # The expiry test lives here
    ///
    /// Not on the request path: a key past its `expires_at` is simply absent
    /// from the set, so authentication is a lookup and the clock is read once
    /// per refresh, not once per request.
    fn load_active(&self) -> Result<Vec<(String, Arc<PartnerRuntimeConfig>)>> {
        self.pool
            .read(|conn| {
                let tx = conn.unchecked_transaction()?;

                // Model capability *and* pricing for every partner, in one
                // query. A model absent here is a model the partner cannot
                // call — there is no second list to consult.
                let mut models: HashMap<String, BTreeMap<String, PricingSnapshot>> = HashMap::new();
                {
                    let mut stmt = tx.prepare(
                        "SELECT consumer_id, model, input_price_micro_usd_per_million, \
                         cached_input_price_micro_usd_per_million, \
                         output_price_micro_usd_per_million \
                         FROM partner_models",
                    )?;
                    let rows = stmt.query_map([], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, String>(1)?,
                            PricingSnapshot::new(
                                PricePerMillion::new(row.get(2)?),
                                PricePerMillion::new(row.get(3)?),
                                PricePerMillion::new(row.get(4)?),
                            ),
                        ))
                    })?;
                    for row in rows {
                        let (consumer_id, model, prices) = row?;
                        models.entry(consumer_id).or_default().insert(model, prices);
                    }
                }

                // The active keys, their partner row, and — for a partner with
                // one — the oldest *complete, non-zero* unpaid invoice that is
                // already past its due date. That subquery is the definition of
                // "what suspends a partner"; `status_for` re-checks the same
                // three facts defensively, and its comment says which of the two
                // is authoritative.
                //
                // A `LEFT JOIN` on `partners` on purpose. A key whose consumer
                // has no partner row is anomalous, and the two ways to react are
                // to stop authenticating it or to let it authenticate with no
                // models. The second is right: a 401 for a key that exists is a
                // lie, whereas a partner who may call nothing fails the model
                // gate, which is both true and visible in the logs.
                let mut stmt = tx.prepare(
                    "SELECT k.key_hash, k.name, k.consumer_id, \
                            COALESCE(p.billing_mode, 'invoice'), \
                            COALESCE(p.billing_email, ''), \
                            s.id, s.billing_date, s.due_at, s.total_amount_micro_usd, \
                            s.incomplete_usage_count \
                     FROM api_keys k \
                     LEFT JOIN partners p ON p.consumer_id = k.consumer_id \
                     LEFT JOIN daily_statements s ON s.id = ( \
                         SELECT d.id FROM daily_statements d \
                         WHERE d.consumer_id = k.consumer_id \
                           AND d.billing_mode = 'invoice' \
                           AND d.paid_at IS NULL \
                           AND d.incomplete_usage_count = 0 \
                           AND d.total_amount_micro_usd > 0 \
                           AND d.due_at IS NOT NULL \
                           AND d.due_at <= ?1 \
                         ORDER BY d.due_at, d.id \
                         LIMIT 1 \
                     ) \
                     WHERE k.status = 'active' \
                       AND (k.expires_at IS NULL OR k.expires_at > ?1) \
                     ORDER BY k.consumer_id",
                )?;
                let now = timefmt::now();
                let now_text = timefmt::format_ts(now);
                let rows = stmt.query_map([&now_text], |row| {
                    let hash: String = row.get(0)?;
                    let name: String = row.get(1)?;
                    let consumer_id: String = row.get(2)?;
                    let mode_text: String = row.get(3)?;
                    let billing_email: String = row.get(4)?;

                    // The column has a CHECK, so an unreadable mode can only
                    // mean a row this product did not write. Refusing to parse
                    // it would take the key out of service over a value nobody
                    // chose; defaulting to `invoice` with an empty model list
                    // leaves the partner unable to call anything and cannot
                    // invent a payment obligation they will be suspended for.
                    let billing_mode =
                        BillingMode::parse(&mode_text).unwrap_or(BillingMode::Invoice);

                    let overdue_id: Option<i64> = row.get(5)?;
                    let overdue = overdue_id
                        .map(|id| -> rusqlite::Result<OverdueRow> {
                            Ok(OverdueRow {
                                billing_mode: BillingMode::Invoice.as_str().to_string(),
                                id,
                                billing_date: row.get(6)?,
                                due_at: row.get(7)?,
                                total_amount_micro_usd: row.get(8)?,
                                incomplete_usage_count: row.get(9)?,
                            })
                        })
                        .transpose()?
                        .into_iter()
                        .collect::<Vec<_>>();

                    let service_status = status_for(&mode_text, now, &overdue);
                    let config = PartnerRuntimeConfig::new(
                        consumer_id.clone(),
                        name,
                        billing_mode,
                        billing_email,
                        service_status,
                        models.remove(&consumer_id).unwrap_or_default(),
                    );
                    Ok((hash, Arc::new(config)))
                })?;
                let collected = rows.collect::<rusqlite::Result<Vec<_>>>()?;
                drop(stmt);

                tx.commit()?;
                Ok(collected)
            })
            .map_err(Into::into)
    }

    /// Issue a new key. The returned plaintext is the only copy that exists.
    ///
    /// A key carries no model list of its own (ADR 0015). Which models the
    /// partner may call, and what each costs, is configured once in
    /// `partner_models` and applies to the partner — not to the credential. A
    /// key created here for a partner with no configured models authenticates
    /// and can call nothing, which is the visible and safe state to be in.
    pub fn create(
        &self,
        name: &str,
        consumer_id: &str,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<(ApiKeyRow, String)> {
        let plaintext = generate_plaintext()
            .map_err(|e| ApiKeyError::Invalid(format!("could not read system entropy: {e}")))?;
        let row = self.create_with_plaintext(name, consumer_id, expires_at, &plaintext)?;
        Ok((row, plaintext))
    }

    /// Issue a key whose plaintext the caller supplies, instead of generating
    /// one.
    ///
    /// # Why this exists
    ///
    /// Two callers, both outside the request path, and neither of them an
    /// alternative to [`create`] on the admin surface — which always generates
    /// its plaintext and can never be asked for a chosen one:
    ///
    /// * **moving an existing key into the database.** A deployment that used
    ///   to hold keys in `config.yaml` can register the same plaintexts here, so
    ///   partners are not re-issued a key by a change of storage. A generated
    ///   replacement would be a credential rotation, which is a different and
    ///   much louder operation than a migration.
    /// * **fixtures.** `scripts/dev-seed-keys.sh` and the test harnesses write
    ///   memorable plaintexts (`dev-key`) so that a developer types one into a
    ///   login screen rather than reading a random string out of a log.
    ///
    /// A supplied plaintext carries whatever entropy the caller gave it, which
    /// is why the admin API does not expose this and why `keygen --plaintext`
    /// says so. Everything else is identical to [`create`]: the plaintext is
    /// hashed and never written, and the returned row carries the prefix only.
    pub fn create_with_plaintext(
        &self,
        name: &str,
        consumer_id: &str,
        expires_at: Option<OffsetDateTime>,
        plaintext: &str,
    ) -> Result<ApiKeyRow> {
        let name = require_text("name", name)?;
        let consumer_id = require_text("consumer_id", consumer_id)?;
        // Blank is rejected; otherwise the value is stored **exactly as given**.
        // Trimming it the way the other fields are trimmed would mint a
        // credential different from the one supplied, and the operator's only
        // symptom would be a partner's 401 that points nowhere. The hashing
        // secret is not trimmed either, for the same reason.
        if plaintext.trim().is_empty() {
            return Err(ApiKeyError::Invalid(
                "plaintext must not be empty".to_string(),
            ));
        }
        let hash = derive_key_hash(&self.secret, plaintext);
        let prefix = key_prefix_of(plaintext);
        let now = timefmt::format_ts(timefmt::now());
        let expires_at = expires_at.map(timefmt::format_ts);

        let created = self
            .mutate_domain(|tx| {
                insert_key(
                    tx,
                    NewKey {
                        name: &name,
                        consumer_id: &consumer_id,
                        prefix: &prefix,
                        hash: &hash,
                        now: &now,
                        expires_at: expires_at.as_deref(),
                    },
                )
                // The partial unique index is the enforcement point, so its
                // violation is translated here rather than pre-checked: a
                // pre-check would be the race the index exists to close.
                //
                // Both conditions are needed. Without the extended-code test a
                // `UNIQUE(key_hash)` collision — the same plaintext issued
                // twice — would be reported as "this consumer already has a
                // key" whenever the consumer happens to have one, which is a
                // wrong diagnosis of a real mistake. Without the `active_key_exists`
                // read, an insert refused by the index could only be reported as
                // a raw constraint failure.
                .map_err(|e| {
                    if unique_violation(&e) && active_key_exists(tx, &consumer_id) {
                        Mutation::Domain(ApiKeyError::AlreadyActive(PartnerKeyExists {
                            consumer_id: consumer_id.clone(),
                        }))
                    } else {
                        Mutation::Sqlite(e)
                    }
                })?;
                Ok(Outcome::Done(fetch_written_row(
                    tx,
                    tx.last_insert_rowid(),
                )?))
            })?
            .done_or_missing()?;

        Ok(created)
    }

    /// One key, or `None`.
    pub fn get(&self, id: i64) -> Result<Option<ApiKeyRow>> {
        self.pool
            .read(|conn| fetch_row(conn, id))
            .map_err(Into::into)
    }

    /// Every key, newest first, revoked ones included.
    pub fn list(&self) -> Result<Vec<ApiKeyRow>> {
        self.pool
            .read(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {SELECT_COLUMNS} FROM api_keys ORDER BY id DESC"
                ))?;
                let rows = stmt.query_map([], row_to_api_key)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(Into::into)
    }

    /// Change a key's name. Never changes the secret, and never changes what
    /// the key may call — that is a property of the partner (ADR 0015).
    pub fn update(&self, id: i64, name: Option<&str>) -> Result<ApiKeyRow> {
        if let Some(name) = name {
            require_text("name", name)?;
        }
        let now = timefmt::format_ts(timefmt::now());

        let outcome = self.mutate(|tx| {
            let Some(current) = fetch_row(tx, id)? else {
                return Ok(Outcome::Missing);
            };
            // A revoked key is immutable. The transaction still commits — it has
            // simply done nothing, so there is nothing to roll back — and the
            // status check below turns that into the caller's error.
            if current.status != KeyStatus::Active {
                return Ok(Outcome::Unchanged(current));
            }

            let new_name = name.unwrap_or(&current.name).to_string();
            tx.execute(
                "UPDATE api_keys SET name = ?2, updated_at = ?3 WHERE id = ?1",
                params![id, new_name, now],
            )?;
            Ok(Outcome::Done(fetch_written_row(tx, id)?))
        })?;

        outcome.result(id, require_active)
    }

    /// Revoke a key. Idempotent: revoking a revoked key reports it as already
    /// revoked rather than failing, because the caller's intent is satisfied.
    pub fn revoke(&self, id: i64) -> Result<ApiKeyRow> {
        let now = timefmt::format_ts(timefmt::now());

        self.mutate(|tx| {
            let Some(current) = fetch_row(tx, id)? else {
                return Ok(Outcome::Missing);
            };
            if current.status == KeyStatus::Active {
                tx.execute(
                    "UPDATE api_keys \
                     SET status = 'revoked', revoked_at = ?2, updated_at = ?2 \
                     WHERE id = ?1",
                    params![id, now],
                )?;
                let row = fetch_written_row(tx, id)?;
                return Ok(Outcome::Done(row));
            }
            Ok(Outcome::Unchanged(current))
        })?
        .result(id, |_| Ok(()))
    }

    /// Replace a key's secret, keeping its identity and history.
    ///
    /// The predecessor is revoked rather than overwritten, in the **same
    /// transaction** as the insert. So a crash mid-rotation can leave the old
    /// key still valid or the new one already valid, but never a state where
    /// the consumer has two live secrets or none.
    pub fn rotate(&self, id: i64) -> Result<(ApiKeyRow, String)> {
        let plaintext = generate_plaintext()
            .map_err(|e| ApiKeyError::Invalid(format!("could not read system entropy: {e}")))?;
        let hash = derive_key_hash(&self.secret, &plaintext);
        let prefix = key_prefix_of(&plaintext);
        let now = timefmt::format_ts(timefmt::now());

        let (_revoked, replacement) = self
            .mutate(|tx| {
                let Some(current) = fetch_row(tx, id)? else {
                    return Ok(Outcome::Missing);
                };
                if current.status != KeyStatus::Active {
                    return Ok(Outcome::Unchanged((current, None)));
                }

                tx.execute(
                    "UPDATE api_keys \
                     SET status = 'revoked', revoked_at = ?2, updated_at = ?2 \
                     WHERE id = ?1",
                    params![id, now],
                )?;
                // A rotation that carried a forward expiry keeps it; one that had
                // none stays without an expiry rather than gaining an arbitrary
                // one.
                insert_key(
                    tx,
                    NewKey {
                        name: &current.name,
                        consumer_id: &current.consumer_id,
                        prefix: &prefix,
                        hash: &hash,
                        now: &now,
                        expires_at: current.expires_at.as_deref(),
                    },
                )?;
                let created = fetch_written_row(tx, tx.last_insert_rowid())?;
                Ok(Outcome::Done((current, Some(created))))
            })?
            .result(id, |(previous, _)| require_active(previous))?;

        // The insert and the revocation are one transaction, so a `None` here
        // cannot mean "the insert did not happen" — only "the key was not
        // active", which `require_active` has already rejected.
        let replacement = replacement.ok_or(ApiKeyError::NotActive(id))?;
        Ok((replacement, plaintext))
    }

    /// Run a write against the single writer connection inside a transaction,
    /// then bring this instance's own snapshot up to date.
    ///
    /// The closure stays in `rusqlite`'s error type and reports a domain
    /// outcome as data, so the SQL layer never has to know about
    /// [`ApiKeyError`]. The transaction commits either way — a closure that
    /// decided not to change anything has nothing to roll back — and the caller
    /// maps the outcome onto an error once the commit is behind us.
    ///
    /// Every mutation goes through here, so a key this instance issued or
    /// revoked is usable (or unusable) on the very next request rather than
    /// after the next poll. Only this instance's snapshot is touched; a sibling
    /// sees the commit on its own refresher tick, which is the propagation delay
    /// recorded in ADR 0014.
    ///
    /// A refresh that fails after a successful commit is **not** rolled back.
    /// The database is the source of truth and the write is durable, so
    /// reporting an error would tell the operator a key was not created when it
    /// was. The snapshot catches up on the next poll; the failure is logged and
    /// the response reports what the database actually holds.
    fn mutate<F, T>(&self, f: F) -> Result<Outcome<T>>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> rusqlite::Result<Outcome<T>>,
    {
        self.mutate_domain(|tx| f(tx).map_err(|e| e.into()))
    }

    /// As [`ApiKeyStore::mutate`], for a transaction that can refuse for a
    /// domain reason and not merely for a SQLite one.
    ///
    /// The split exists because the unique index is the *enforcement point* for
    /// one key per partner, and the database cannot raise a domain error. So
    /// the transaction returns `AlreadyActive` and it is translated here, after
    /// the rollback that discards a half-written insert has already happened. A
    /// domain error raised *before* the commit is a failure of the whole
    /// transaction, which is the right outcome for a refused create — nothing
    /// was written, so there is no useful work to abandon.
    fn mutate_domain<F, T>(&self, f: F) -> Result<Outcome<T>>
    where
        F: FnOnce(&rusqlite::Transaction<'_>) -> std::result::Result<Outcome<T>, Mutation>,
    {
        // A domain refusal leaves through this `Option`, out of band.
        //
        // `LedgerPool::write` is pinned to `rusqlite::Result`, and a refusal
        // smuggled through an error variant comes back out as "database error"
        // with the reason lost — which is exactly what the split into `Mutation`
        // exists to prevent, and is what a first version of this did. The
        // transaction is rolled back by its `Drop` on the way out, so the
        // stand-in error only has to end the closure without committing.
        let mut refused: Option<ApiKeyError> = None;

        let outcome = self.pool.write(|conn| {
            let tx = conn.transaction()?;
            let out = match f(&tx) {
                Ok(out) => out,
                Err(Mutation::Sqlite(e)) => return Err(e),
                Err(Mutation::Domain(domain)) => {
                    refused = Some(domain);
                    return Err(rusqlite::Error::InvalidQuery);
                }
            };
            tx.commit()?;
            Ok(out)
        });

        let outcome = match (refused, outcome) {
            // Set and returned together, so `Some` here means the closure did
            // not commit and the failure is this refusal, not the stand-in.
            (Some(domain), _) => return Err(domain),
            (None, Ok(out)) => out,
            (None, Err(e)) => return Err(ApiKeyError::from(e)),
        };

        if let Err(e) = self.refresh() {
            tracing::error!(
                error = %e,
                "api key change committed but the in-memory snapshot could not \
                 be reloaded; it will catch up on the next refresh"
            );
        }
        Ok(outcome)
    }
}

/// Why a mutation did not happen.
///
/// A domain refusal and a SQLite failure are different things: one is a
/// decision, the other is an accident, and only the second deserves a stack
/// trace in a log. Neither is ever wrapped into the other, because a
/// constraint violation that has been given a name must not also be reported
/// as "database error" — an operator reading a 500 learns nothing.
///
/// There used to be a `From<ApiKeyError> for rusqlite::Error` that boxed a
/// refusal into `ToSqlConversionFailure` so it could cross the `rusqlite::Result`
/// boundary `LedgerPool::write` is pinned to. It propagated — the write failed
/// and rolled back — but it came back out as `Database(..)`: the 409 the admin
/// API renders became a 500, and "this partner already has an active key",
/// which is actionable, became "database error", which is not. A conversion
/// whose only reader cannot recover what it wrote is not worth keeping, so the
/// refusal is now held in an `Option` beside the closure (see `mutate_domain`).
enum Mutation {
    Sqlite(rusqlite::Error),
    Domain(ApiKeyError),
}

impl From<rusqlite::Error> for Mutation {
    fn from(e: rusqlite::Error) -> Self {
        Mutation::Sqlite(e)
    }
}

impl From<ApiKeyError> for Mutation {
    fn from(e: ApiKeyError) -> Self {
        Mutation::Domain(e)
    }
}

/// A domain refusal is carried out of a transaction body in a variable, not in
/// an error.
///
/// What a mutation did, as data rather than as an error.
///
/// `pool.write` is pinned to `rusqlite::Result`, so a domain error cannot cross
/// the write boundary — and it should not, because "the key is already revoked"
/// is not a reason to abandon a transaction that may have done useful work.
#[derive(Debug)]
enum Outcome<T> {
    /// The row was written.
    Done(T),
    /// No row has that id.
    Missing,
    /// The row exists but the operation deliberately left it alone.
    Unchanged(T),
}

impl<T> Outcome<T> {
    /// Collapse to a value, applying `check` to whatever the mutation found.
    fn result<F>(self, id: i64, check: F) -> Result<T>
    where
        F: FnOnce(&T) -> Result<()>,
    {
        match self {
            Outcome::Done(value) | Outcome::Unchanged(value) => {
                check(&value)?;
                Ok(value)
            }
            Outcome::Missing => Err(ApiKeyError::NotFound(id)),
        }
    }

    /// [`Outcome::result`] for the paths that cannot produce `Missing`.
    fn done_or_missing(self) -> Result<T> {
        match self {
            Outcome::Done(value) | Outcome::Unchanged(value) => Ok(value),
            Outcome::Missing => Err(ApiKeyError::NotFound(0)),
        }
    }
}

/// Refuse an operation on a key that has already been revoked.
fn require_active(row: &ApiKeyRow) -> Result<()> {
    if row.status == KeyStatus::Active {
        Ok(())
    } else {
        Err(ApiKeyError::NotActive(row.id))
    }
}

/// Log a partner gaining or losing service between two snapshots.
///
/// # Why the status is only reported on change
///
/// Suspension is derived, and the refresh runs once a second, so "is this
/// partner suspended" is a question asked ten thousand times a day with the same
/// answer. Only the *crossing* is an event: a partner who was being served and
/// now is not has had their calls start failing, and that is worth waking
/// someone for. Logging the state instead of the transition would bury it.
///
/// # Why a hash absent from one side is not a transition
///
/// The snapshot is keyed by the key hash that authenticates, and the two sides
/// are a second apart. A key created, revoked or rotated in between changes the
/// hash without anything having happened to the partner's service. So only
/// hashes present in *both* snapshots are compared — a revoked key vanishing is
/// not a resume, and reading it as one would log exactly the opposite of what
/// happened at the moment it matters most.
///
/// A hash that appears for the first time and is already suspended *is*
/// reported: that is a bill going unpaid and a fresh credential arriving to
/// carry it, and an operator should see it.
///
/// The message *is* the event name (`partner_suspended`, `partner_resumed`), not
/// a sentence about it. These two are operational signals something watches for,
/// and `grep partner_suspended` has to find them; the fields beside it carry what
/// a human needs to act.
fn log_service_transitions(previous: &PartnerSnapshot, next: &PartnerSnapshot) {
    let before: HashMap<&str, &Arc<PartnerRuntimeConfig>> = previous.entries().collect();

    for (hash, config) in next.entries() {
        let was_suspended = match before.get(hash) {
            Some(old) => {
                if old.is_suspended() == config.is_suspended() {
                    // Most of the time, and the whole point of this function.
                    continue;
                }
                Some(old.is_suspended())
            }
            None => None,
        };

        match (&config.service_status, was_suspended) {
            (ServiceStatus::Suspended { reason }, Some(false))
            | (ServiceStatus::Suspended { reason }, None) => {
                tracing::warn!(
                    consumer_id = %config.consumer_id,
                    status = %config.service_status,
                    reason = ?reason,
                    "partner_suspended"
                );
            }
            (ServiceStatus::Active, Some(true)) => {
                tracing::info!(
                    consumer_id = %config.consumer_id,
                    "partner_resumed"
                );
            }
            // A new active key, or a state this match already handled.
            _ => {}
        }
    }
}

/// A new key, ready to be written.
///
/// Grouped rather than passed as eight arguments because `create` and `rotate`
/// build the same thing from different places, and a positional list is where
/// those two paths would silently transpose `name` and `consumer_id`.
struct NewKey<'a> {
    name: &'a str,
    consumer_id: &'a str,
    prefix: &'a str,
    hash: &'a str,
    now: &'a str,
    expires_at: Option<&'a str>,
}

/// Insert one key. Shared by `create` and `rotate` so they cannot drift apart.
fn insert_key(conn: &Connection, key: NewKey<'_>) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO api_keys \
         (name, consumer_id, key_prefix, key_hash, status, \
          created_at, updated_at, expires_at) \
         VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?5, ?6)",
        params![
            key.name,
            key.consumer_id,
            key.prefix,
            key.hash,
            key.now,
            key.expires_at
        ],
    )?;
    Ok(())
}

/// `SELECT *` order must match [`row_to_api_key`]; the two are one list.
const SELECT_COLUMNS: &str = "id, name, consumer_id, key_prefix, key_hash, \
     status, created_at, updated_at, expires_at, revoked_at";

fn fetch_row(conn: &Connection, id: i64) -> rusqlite::Result<Option<ApiKeyRow>> {
    conn.query_row(
        &format!("SELECT {SELECT_COLUMNS} FROM api_keys WHERE id = ?1"),
        [id],
        row_to_api_key,
    )
    .optional()
}

/// [`fetch_row`] for a row this transaction has just written.
///
/// It cannot be missing, so `None` means the schema and the query disagree —
/// which is a defect, and `QueryReturnedNoRows` reports it as one. Returning
/// `Option` here would push the same judgement onto every call site, where the
/// only sensible response is to fabricate a row or to panic.
fn fetch_written_row(conn: &Connection, id: i64) -> rusqlite::Result<ApiKeyRow> {
    fetch_row(conn, id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)
}

/// Map a row. Never reads the plaintext — there is none — and never surfaces
/// the hash, which would leak the digest into a response body or a log line.
fn row_to_api_key(row: &Row<'_>) -> rusqlite::Result<ApiKeyRow> {
    let status: String = row.get(5)?;
    Ok(ApiKeyRow {
        id: row.get(0)?,
        name: row.get(1)?,
        consumer_id: row.get(2)?,
        key_prefix: row.get(3)?,
        status: KeyStatus::parse(&status).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(5, "status".to_string(), rusqlite::types::Type::Text)
        })?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        expires_at: row.get(8)?,
        revoked_at: row.get(9)?,
    })
}

fn require_text(field: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiKeyError::Invalid(format!("{field} must not be empty")));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::partner::ModelPrice;
    use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
    use crate::billing::store::{BillingStore, NewPartner};
    use crate::ledger::LedgerPool;
    use time::Duration;

    const SECRET: &[u8] = b"a-store-test-secret-of-32-bytes!!!";

    fn store() -> (tempfile::TempDir, ApiKeyStore) {
        let dir = tempfile::TempDir::new().unwrap();
        let pool = LedgerPool::new(dir.path().join("ledger.db")).unwrap();
        let store = ApiKeyStore::new(Arc::new(pool), SECRET.to_vec());
        (dir, store)
    }

    fn models(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// Configure a partner and its models, the way the admin API does.
    ///
    /// A key carries no model list of its own (ADR 0015), so a test that wants
    /// a partner that may call something has to say so here — and the price is
    /// part of the same row, which is why this takes one number per model and
    /// uses it for all three components.
    fn partner(store: &ApiKeyStore, consumer: &str, list: &[(&str, i64)]) {
        let billing = BillingStore::new(Arc::clone(store.pool()));
        if billing.get_partner(consumer).unwrap().is_some() {
            // Idempotent: several tests create two keys for one consumer, and
            // the second call is a reconfiguration of the same partner rather
            // than a new one.
            return;
        }
        billing
            .create_partner(NewPartner {
                consumer_id: consumer.to_string(),
                name: consumer.to_string(),
                billing_email: format!("billing@{consumer}.test"),
                billing_mode: BillingMode::Invoice,
                payment_terms_minutes: crate::billing::DEFAULT_PAYMENT_TERMS_MINUTES,
            })
            .expect("the partner must be created");
        let rows: Vec<ModelPrice> = list
            .iter()
            .map(|(name, price)| ModelPrice {
                model: (*name).to_string(),
                prices: PricingSnapshot::new(
                    PricePerMillion::new(*price),
                    PricePerMillion::new(*price),
                    PricePerMillion::new(*price),
                ),
            })
            .collect();
        billing
            .replace_models(consumer, &rows)
            .expect("the models must be written");
    }

    /// A store whose snapshot is loaded, which is what startup does.
    fn loaded() -> (tempfile::TempDir, ApiKeyStore) {
        let (dir, store) = store();
        store
            .refresh()
            .expect("a fresh database has no keys to load");
        (dir, store)
    }

    /// A partner with one model and a key for it — the ordinary case.
    fn create(store: &ApiKeyStore, name: &str, consumer: &str) -> (ApiKeyRow, String) {
        partner(store, consumer, &[("gpt-4o", 95_000)]);
        store
            .create(name, consumer, None)
            .expect("create must succeed")
    }

    #[test]
    fn test_created_key_authenticates_with_its_plaintext_and_nothing_else() {
        let (_dir, store) = loaded();
        let (row, plaintext) = create(&store, "primary", "acme");

        let auth = store
            .authenticate(&plaintext)
            .expect("the key must authenticate");
        assert_eq!(auth.consumer_id, "acme");
        assert_eq!(auth.key_name, "primary");
        assert_eq!(auth.allowed_models(), models(&["gpt-4o"]));

        // Nothing else does: not the prefix, not the name, not the row id.
        assert!(store.authenticate(&row.key_prefix).is_none());
        assert!(store.authenticate("primary").is_none());
        assert!(
            store
                .authenticate(&plaintext[..plaintext.len() - 1])
                .is_none()
        );
        assert!(store.authenticate(&format!("{plaintext}x")).is_none());
        assert!(store.authenticate("").is_none());
    }

    #[test]
    fn test_a_different_secret_cannot_authenticate_a_key_issued_under_this_one() {
        let (_dir, store) = loaded();
        let (_row, plaintext) = create(&store, "primary", "acme");

        // A second store over the *same database* with a different secret sees
        // the same rows and authenticates nothing. This is the property that
        // makes a stolen database inert.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ledger.db");
        {
            let first = ApiKeyStore::new(
                Arc::new(LedgerPool::new(path.clone()).unwrap()),
                SECRET.to_vec(),
            );
            first.create("primary", "acme", None).unwrap();
        }
        let other = ApiKeyStore::new(
            Arc::new(LedgerPool::new(path).unwrap()),
            b"a-completely-different-secret-of-32b".to_vec(),
        );
        other.refresh().unwrap();
        assert!(
            other.authenticate(&plaintext).is_none(),
            "a hash keyed with another secret must not verify"
        );
    }

    #[test]
    fn test_revoking_a_key_stops_it_authenticating_after_a_refresh() {
        let (_dir, store) = loaded();
        let (row, plaintext) = create(&store, "primary", "acme");
        assert!(store.authenticate(&plaintext).is_some());

        let revoked = store.revoke(row.id).expect("revoke must succeed");
        assert_eq!(revoked.status, KeyStatus::Revoked);
        assert!(revoked.revoked_at.is_some(), "a revoked key says when");

        // The snapshot is what authenticates, and the mutation reloads it, so
        // the revocation is effective immediately on this instance.
        assert!(store.authenticate(&plaintext).is_none());

        // Revoking again is not an error: the caller's intent is satisfied.
        let again = store.revoke(row.id).expect("revoking twice must not fail");
        assert_eq!(again.status, KeyStatus::Revoked);
        assert_eq!(again.revoked_at, revoked.revoked_at, "no second timestamp");
    }

    #[test]
    fn test_rotation_replaces_the_secret_and_keeps_the_consumer() {
        let (_dir, store) = loaded();
        let (original, old_plaintext) = create(&store, "primary", "acme");
        let (rotated, new_plaintext) = store.rotate(original.id).expect("rotate must succeed");

        assert_ne!(rotated.id, original.id, "rotation issues a new row");
        assert_eq!(rotated.consumer_id, "acme", "identity is preserved");
        assert_eq!(rotated.name, "primary");
        assert_ne!(rotated.key_prefix, original.key_prefix);
        assert_ne!(new_plaintext, old_plaintext);

        store.refresh().unwrap();
        assert!(
            store.authenticate(&old_plaintext).is_none(),
            "the rotated-out secret must stop working"
        );
        let auth = store
            .authenticate(&new_plaintext)
            .expect("the new secret must work");
        assert_eq!(auth.consumer_id, "acme");
        // Rotation changes the secret and nothing else: the successor resolves
        // to the same partner configuration, because that configuration is the
        // partner's (ADR 0015) and a rotation that changed it would silently
        // change what the partner may call and what they pay.
        assert_eq!(auth.allowed_models(), models(&["gpt-4o"]));
        assert_eq!(auth.pricing_for("gpt-4o").unwrap().input.as_i64(), 95_000);

        // The predecessor is retained as history, not overwritten.
        let previous = store
            .get(original.id)
            .unwrap()
            .expect("the old row survives");
        assert_eq!(previous.status, KeyStatus::Revoked);
        assert_eq!(previous.consumer_id, "acme");
    }

    #[test]
    fn test_rotation_is_refused_for_a_revoked_key() {
        let (_dir, store) = loaded();
        let (row, _) = create(&store, "primary", "acme");
        store.revoke(row.id).unwrap();

        assert!(matches!(
            store.rotate(row.id),
            Err(ApiKeyError::NotActive(1))
        ));

        // The refusal must not have issued a replacement.
        assert_eq!(store.active_count(), 0);
    }

    #[test]
    fn test_rotation_keeps_a_forward_expiry_and_does_not_invent_one() {
        let (_dir, store) = loaded();
        let expiry = timefmt::now() + Duration::days(7);

        partner(&store, "expiring-partner", &[("gpt-4o", 95_000)]);
        let (expiring, _) = store
            .create("expiring", "expiring-partner", Some(expiry))
            .unwrap();
        let (rotated, _) = store.rotate(expiring.id).unwrap();
        assert_eq!(
            rotated.expires_at,
            Some(timefmt::format_ts(expiry)),
            "a rotation inherits the deadline it was given"
        );

        let (perpetual, _) = create(&store, "perpetual", "perpetual-partner");
        let (rotated, _) = store.rotate(perpetual.id).unwrap();
        assert_eq!(rotated.expires_at, None, "no expiry means none is added");
    }

    #[test]
    fn test_an_expired_key_is_not_authenticatable() {
        let (_dir, store) = loaded();
        partner(&store, "expiring-partner", &[("gpt-4o", 95_000)]);
        partner(&store, "active-partner", &[("gpt-4o", 95_000)]);
        let (past, _) = store
            .create(
                "expired",
                "expiring-partner",
                Some(timefmt::now() - Duration::hours(1)),
            )
            .unwrap();
        let (_future, future_plaintext) = store
            .create(
                "valid",
                "active-partner",
                Some(timefmt::now() + Duration::hours(1)),
            )
            .unwrap();

        // Expiry is a load-time predicate, so the row still exists and still
        // reports itself active — it simply is not in the authenticating set.
        // It also still occupies the partner's one active slot, which is
        // deliberate: the row is what a partner would rotate or revoke, and
        // letting an expired key be silently displaced by a new one would mean
        // the old key's row changed status without anyone asking.
        assert_eq!(
            store.get(past.id).unwrap().unwrap().status,
            KeyStatus::Active
        );
        assert_eq!(store.active_count(), 1);
        assert!(store.authenticate(&future_plaintext).is_some());
        assert!(store.authenticate("pp_expired").is_none());
    }

    #[test]
    fn test_update_renames_a_key_and_never_the_partner_configuration() {
        // A key's update surface is its label. What the partner may call and
        // what each model costs are not properties of a credential, and the
        // method takes no argument that could change them (ADR 0015) — this
        // test is here so that adding one is a deliberate act rather than a
        // convenience.
        let (_dir, store) = loaded();
        let (row, plaintext) = create(&store, "primary", "acme");

        let updated = store
            .update(row.id, Some("renamed"))
            .expect("update must succeed");
        assert_eq!(updated.name, "renamed");
        assert_eq!(updated.consumer_id, "acme");

        let auth = store.authenticate(&plaintext).expect("the key still works");
        assert_eq!(
            auth.key_name, "renamed",
            "the new name is what a request sees"
        );
        assert_eq!(auth.allowed_models(), models(&["gpt-4o"]));
        assert_eq!(auth.pricing_for("gpt-4o").unwrap().input.as_i64(), 95_000);
    }

    #[test]
    fn test_update_is_refused_for_a_revoked_key_and_changes_nothing() {
        let (_dir, store) = loaded();
        let (row, _) = create(&store, "primary", "acme");
        store.update(row.id, Some("renamed")).unwrap();
        store.revoke(row.id).unwrap();

        assert!(matches!(
            store.update(row.id, Some("sneaky")),
            Err(ApiKeyError::NotActive(1))
        ));
        assert_eq!(store.get(row.id).unwrap().unwrap().name, "renamed");
    }

    #[test]
    fn test_missing_ids_are_not_found_rather_than_silently_created() {
        let (_dir, store) = loaded();
        assert!(store.get(404).unwrap().is_none());
        assert!(matches!(store.revoke(404), Err(ApiKeyError::NotFound(404))));
        assert!(matches!(store.rotate(404), Err(ApiKeyError::NotFound(404))));
        assert!(matches!(
            store.update(404, Some("x")),
            Err(ApiKeyError::NotFound(404))
        ));
        assert_eq!(
            store.list().unwrap().len(),
            0,
            "a failed op creates nothing"
        );
    }

    /// The model gate, now that it comes from the partner's own price list.
    ///
    /// A configured list admits exactly its members; matching is literal, not
    /// case-insensitive; a model absent from `partner_models` has no permission
    /// *and* no price; and a partner with no rows may call nothing — never
    /// "everything". That last one is the dangerous direction, so it is asserted
    /// through the snapshot the request path actually reads.
    #[test]
    fn test_the_model_gate_is_strict_exact_and_comes_with_a_price() {
        let (_dir, store) = loaded();
        partner(
            &store,
            "listed-partner",
            &[("gpt-4o", 95_000), ("gpt-4o-mini", 15_000)],
        );
        let (_listed, plaintext) = store.create("listed", "listed-partner", None).unwrap();

        let auth = store.authenticate(&plaintext).unwrap();
        assert!(auth.allows("gpt-4o"));
        assert!(auth.allows("gpt-4o-mini"));
        assert!(!auth.allows("gpt-5"), "an unlisted model has no permission");
        assert!(
            !auth.allows("GPT-4o"),
            "matching is exact, not case-insensitive"
        );
        assert_eq!(auth.pricing_for("gpt-5"), None, "and no price either");
        assert_eq!(
            auth.pricing_for("gpt-4o-mini").unwrap().input.as_i64(),
            15_000
        );

        // A partner with no rows at all: no permission, and no price.
        let (_empty, plaintext) = store.create("empty", "empty-partner", None).unwrap();
        let auth = store.authenticate(&plaintext).unwrap();
        assert!(auth.allowed_models().is_empty());
        assert!(!auth.allows("gpt-4o"));
        assert_eq!(auth.pricing_for("gpt-4o"), None);
    }

    #[test]
    fn test_empty_fields_are_refused_before_they_reach_sql() {
        let (_dir, store) = loaded();
        for bad in ["", "   ", "\t\n"] {
            assert!(matches!(
                store.create(bad, "acme", None),
                Err(ApiKeyError::Invalid(_))
            ));
            assert!(matches!(
                store.create("name", bad, None),
                Err(ApiKeyError::Invalid(_))
            ));
        }
        assert_eq!(store.list().unwrap().len(), 0);
    }

    #[test]
    fn test_a_supplied_plaintext_is_stored_verbatim_and_authenticates() {
        // The migration and fixture path: `keygen --plaintext` and the dev seed
        // register a value the operator already has. The value must come back
        // out of authentication *exactly*, because the operator is about to
        // hand the same string to a partner or type it into a login screen.
        let (_dir, store) = loaded();

        // Deliberately not trimmed: the hashing secret is not trimmed either,
        // and silently altering a credential produces a 401 that points nowhere.
        let supplied = "  dev-key-with-spaces  ";
        store
            .create_with_plaintext("fixture", "acme", None, supplied)
            .unwrap();
        store.refresh().unwrap();
        assert!(
            store.authenticate(supplied).is_some(),
            "the supplied value must authenticate as itself"
        );
        assert!(
            store.authenticate("dev-key-with-spaces").is_none(),
            "and a trimmed version of it must not — the value is stored as given, \
             not normalised"
        );

        // Blank is still refused, and nothing was written.
        for bad in ["", "   ", "\t\n"] {
            assert!(matches!(
                store.create_with_plaintext("fixture", "acme", None, bad),
                Err(ApiKeyError::Invalid(_))
            ));
        }
        assert_eq!(store.list().unwrap().len(), 1);
    }

    #[test]
    fn test_a_supplied_plaintext_cannot_be_registered_twice() {
        // Two rows with one hash would make authentication ambiguous, so the
        // UNIQUE constraint refuses it. Worth asserting rather than assuming:
        // it is what makes `scripts/dev-seed-keys.sh` check before it seeds,
        // and what stops a migration run twice from silently issuing a second
        // live key for the same credential.
        let (_dir, store) = loaded();
        store
            .create_with_plaintext("first", "acme", None, "dev-key")
            .unwrap();
        assert!(matches!(
            store.create_with_plaintext("again", "beta", None, "dev-key"),
            Err(ApiKeyError::Database(_))
        ));
        assert_eq!(store.list().unwrap().len(), 1, "and it wrote nothing");
    }

    #[test]
    fn test_list_includes_revoked_keys_newest_first_and_never_a_secret() {
        let (_dir, store) = loaded();
        let (first, first_plaintext) = create(&store, "first", "acme");
        let (second, second_plaintext) = create(&store, "second", "globex");
        store.revoke(first.id).unwrap();

        let listed = store.list().unwrap();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, second.id, "newest first");
        assert_eq!(listed[1].status, KeyStatus::Revoked);

        // Nothing that leaves the store carries a secret or its digest.
        let rendered = format!("{listed:?}");
        for plaintext in [&first_plaintext, &second_plaintext] {
            assert!(!rendered.contains(plaintext), "a plaintext leaked");
        }
        assert!(!rendered.contains(&crate::apikeys::derive_key_hash(SECRET, &first_plaintext)));
    }

    #[test]
    fn test_schema_rejects_a_row_that_would_break_the_guarantees() {
        let (_dir, store) = store();
        let (row, _) = create(&store, "primary", "acme");

        // Written past the repository on purpose: these are the states the
        // constraints must refuse even if a future bug, a hand-edited row or a
        // second writer tries to produce one. Every case must fail at the
        // schema, not merely be absent from the snapshot.
        //
        // Each is a `(what it is, SQL, bound parameters)` triple; the SQL is a
        // string because the point is that the schema refuses these, not that
        // the repository can express them.
        type Case = (&'static str, &'static str, Vec<Box<dyn rusqlite::ToSql>>);

        let id = || -> Vec<Box<dyn rusqlite::ToSql>> { vec![Box::new(row.id)] };
        let cases: Vec<Case> = vec![
            (
                "a revoked key with no revocation time",
                "UPDATE api_keys SET status='revoked' WHERE id=?1",
                id(),
            ),
            (
                "an active key carrying a revocation time",
                "UPDATE api_keys SET revoked_at='2026-01-01T00:00:00.000000000Z' WHERE id=?1",
                id(),
            ),
            (
                "an unknown status",
                "UPDATE api_keys SET status='pending' WHERE id=?1",
                id(),
            ),
            (
                "a blank model name in a partner's price list",
                "INSERT INTO partner_models (consumer_id, model, \
                 input_price_micro_usd_per_million, \
                 cached_input_price_micro_usd_per_million, \
                 output_price_micro_usd_per_million, created_at, updated_at) \
                 VALUES ('acme', '   ', 0, 0, 0, '2026-01-01T00:00:00.000000000Z', \
                         '2026-01-01T00:00:00.000000000Z')",
                Vec::new(),
            ),
            (
                "a negative price",
                "INSERT INTO partner_models (consumer_id, model, \
                 input_price_micro_usd_per_million, \
                 cached_input_price_micro_usd_per_million, \
                 output_price_micro_usd_per_million, created_at, updated_at) \
                 VALUES ('acme', 'gpt-4o', -1, 0, 0, '2026-01-01T00:00:00.000000000Z', \
                         '2026-01-01T00:00:00.000000000Z')",
                Vec::new(),
            ),
            (
                "a duplicated key hash",
                "INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, \
                 created_at, updated_at) \
                 SELECT 'dup', consumer_id, 'pp_dup', key_hash, created_at, created_at \
                 FROM api_keys WHERE id = ?1",
                id(),
            ),
        ];

        let outcome = store.pool.write(|conn| {
            for (what, sql, params) in &cases {
                let refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
                assert!(
                    conn.execute(sql, refs.as_slice()).is_err(),
                    "the schema must reject {what}"
                );
            }
            Ok(())
        });
        assert!(outcome.is_ok());
    }

    #[test]
    fn test_two_keys_for_one_consumer_share_the_identity_but_not_the_secret() {
        let (_dir, store) = loaded();
        let (primary, primary_plaintext) = create(&store, "primary", "shared");
        let a = store.authenticate(&primary_plaintext).unwrap();
        assert_eq!(a.consumer_id, "shared");

        // A second active key for one consumer is now a database error, not a
        // supported pattern: a partner is a commercial account with one
        // credential, and two live ones would mean two keys that share an
        // invoice and no way to say which is the partner's. What the old
        // behaviour enabled — revoking one and keeping the other working — is
        // exactly what `rotate` does, in one transaction.
        let err = store
            .create("secondary", "shared", None)
            .expect_err("a partner may not have two active keys");
        assert!(
            matches!(
                err,
                ApiKeyError::AlreadyActive(PartnerKeyExists { ref consumer_id })
                    if consumer_id == "shared"
            ),
            "the conflict must name the consumer, not leak a SQLite message: {err}"
        );

        // And the refusal changed nothing: the first key still works.
        assert!(store.authenticate(&primary_plaintext).is_some());
        assert_eq!(store.active_count(), 1);

        // The supported path: rotate. Exactly one active key survives, the new
        // plaintext authenticates and the old one does not.
        let (rotated, rotated_plaintext) = store.rotate(primary.id).unwrap();
        assert_eq!(rotated.consumer_id, "shared");
        assert!(store.authenticate(&rotated_plaintext).is_some());
        assert!(
            store.authenticate(&primary_plaintext).is_none(),
            "rotation must retire the predecessor"
        );
        assert_eq!(store.active_count(), 1);
    }

    #[test]
    fn test_revoking_a_key_frees_the_partner_slot_for_a_new_one() {
        // The counterpart to the refusal above: the constraint is about
        // *active* keys, so the history of every key a partner ever had is
        // kept and a new credential can be issued in its place.
        let (_dir, store) = loaded();
        let (first, _) = create(&store, "first", "acme");
        store.revoke(first.id).unwrap();

        let (second, plaintext) = create(&store, "second", "acme");
        assert_eq!(second.consumer_id, "acme");
        assert!(store.authenticate(&plaintext).is_some());
        assert_eq!(store.active_count(), 1);

        // Both rows are still there: the revoked one is history, not garbage.
        let all = store.list().unwrap();
        assert_eq!(all.len(), 2);
        assert!(
            all.iter()
                .any(|k| k.id == first.id && k.status == KeyStatus::Revoked)
        );
    }

    #[test]
    fn test_snapshot_is_replaced_wholesale_never_merged() {
        let (_dir, store) = loaded();
        // Two keys for two different partners: the shape of the dataset is
        // irrelevant to what this test is about, which is replacement and not
        // accumulation.
        create(&store, "a", "acme");
        create(&store, "b", "globex");
        store.refresh().unwrap();
        assert_eq!(store.active_count(), 2);

        // A key deleted outside the repository must disappear from the set on
        // the next refresh; a merge would keep it alive forever.
        store
            .pool
            .write(|conn| conn.execute("DELETE FROM api_keys", []))
            .unwrap();
        store.refresh().unwrap();
        assert_eq!(store.active_count(), 0);
        assert!(store.snapshot().read().is_empty());
    }
}
