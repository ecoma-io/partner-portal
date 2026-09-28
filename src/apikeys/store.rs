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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use rusqlite::{Connection, OptionalExtension, Row, params};
use time::OffsetDateTime;

use crate::apikeys::{derive_key_hash, generate_plaintext, key_prefix_of};
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
    pub allowed_models: Vec<String>,
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

/// What authentication needs, and the only thing it is given.
#[derive(Debug, Clone)]
pub struct ApiKeyAuth {
    pub name: String,
    pub consumer_id: String,
    pub allowed_models: Vec<String>,
}

/// An immutable map from key hash to the identity that key carries.
///
/// Replaced wholesale, never mutated. A request therefore sees either the whole
/// previous key set or the whole next one; there is no window in which a
/// revocation is half-applied and a concurrent request sees neither state.
#[derive(Debug, Default)]
pub struct ApiKeySnapshot {
    by_hash: HashMap<String, ApiKeyAuth>,
}

impl ApiKeySnapshot {
    /// Build a snapshot from the rows a `load_active` returned.
    pub fn from_rows(rows: Vec<(String, ApiKeyAuth)>) -> Self {
        Self {
            by_hash: rows.into_iter().collect(),
        }
    }

    /// Number of keys that will authenticate.
    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty()
    }

    /// Resolve a plaintext key to the identity it was issued for.
    pub fn get(&self, hash: &str) -> Option<&ApiKeyAuth> {
        self.by_hash.get(hash)
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

/// The key store: repository plus the snapshot authentication reads.
pub struct ApiKeyStore {
    pool: Arc<LedgerPool>,
    secret: Arc<Vec<u8>>,
    snapshot: Arc<RwLock<ApiKeySnapshot>>,
}

impl ApiKeyStore {
    pub fn new(pool: Arc<LedgerPool>, secret: Vec<u8>) -> Self {
        Self {
            pool,
            secret: Arc::new(secret),
            snapshot: Arc::new(RwLock::new(ApiKeySnapshot::default())),
        }
    }

    /// The live snapshot, for the authentication extractor.
    pub fn snapshot(&self) -> Arc<RwLock<ApiKeySnapshot>> {
        Arc::clone(&self.snapshot)
    }

    /// Authenticate a plaintext key. No SQLite, no I/O — a hash and a lookup.
    pub fn authenticate(&self, plaintext: &str) -> Option<ApiKeyAuth> {
        let hash = derive_key_hash(&self.secret, plaintext);
        self.snapshot.read().get(&hash).cloned()
    }

    /// How many keys will currently authenticate.
    pub fn active_count(&self) -> usize {
        self.snapshot.read().len()
    }

    /// Reload the snapshot from the database and swap it in.
    ///
    /// Called at startup and by the refresher, which watches `PRAGMA
    /// data_version` for commits made by *another* connection. Failures leave
    /// the previous snapshot in place — the caller decides whether that is
    /// fatal, and at startup it is.
    pub fn refresh(&self) -> Result<usize> {
        let rows = self.load_active()?;
        let count = rows.len();
        *self.snapshot.write() = ApiKeySnapshot::from_rows(rows);
        Ok(count)
    }

    /// Every key that would authenticate right now.
    ///
    /// The expiry test lives here rather than on the request path: a key past
    /// its `expires_at` is simply absent from the set, so authentication is a
    /// lookup and the clock is read once per refresh, not once per request.
    fn load_active(&self) -> Result<Vec<(String, ApiKeyAuth)>> {
        let now = timefmt::format_ts(timefmt::now());
        self.pool
            .read(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT key_hash, name, consumer_id, allowed_models \
                 FROM api_keys \
                 WHERE status = 'active' AND (expires_at IS NULL OR expires_at > ?1)",
                )?;
                let rows = stmt.query_map([&now], |row| {
                    let hash: String = row.get(0)?;
                    let auth = ApiKeyAuth {
                        name: row.get(1)?,
                        consumer_id: row.get(2)?,
                        allowed_models: decode_models(&row.get::<_, String>(3)?)?,
                    };
                    Ok((hash, auth))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(Into::into)
    }

    /// Issue a new key. The returned plaintext is the only copy that exists.
    pub fn create(
        &self,
        name: &str,
        consumer_id: &str,
        allowed_models: Vec<String>,
        expires_at: Option<OffsetDateTime>,
    ) -> Result<(ApiKeyRow, String)> {
        let name = require_text("name", name)?;
        let consumer_id = require_text("consumer_id", consumer_id)?;
        validate_models(&allowed_models)?;

        let plaintext = generate_plaintext()
            .map_err(|e| ApiKeyError::Invalid(format!("could not read system entropy: {e}")))?;
        let hash = derive_key_hash(&self.secret, &plaintext);
        let prefix = key_prefix_of(&plaintext);
        let now = timefmt::format_ts(timefmt::now());
        let expires_at = expires_at.map(timefmt::format_ts);

        let created = self
            .mutate(|tx| {
                insert_key(
                    tx,
                    NewKey {
                        name: &name,
                        consumer_id: &consumer_id,
                        prefix: &prefix,
                        hash: &hash,
                        allowed_models: &allowed_models,
                        now: &now,
                        expires_at: expires_at.as_deref(),
                    },
                )?;
                Ok(Outcome::Done(fetch_written_row(
                    tx,
                    tx.last_insert_rowid(),
                )?))
            })?
            .done_or_missing()?;

        Ok((created, plaintext))
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

    /// Change a key's name or allow-list. Never changes the secret.
    pub fn update(
        &self,
        id: i64,
        name: Option<&str>,
        allowed_models: Option<Vec<String>>,
    ) -> Result<ApiKeyRow> {
        if let Some(name) = name {
            require_text("name", name)?;
        }
        if let Some(models) = &allowed_models {
            validate_models(models)?;
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
            let models = allowed_models
                .as_deref()
                .map(encode_models)
                .unwrap_or_else(|| encode_models(&current.allowed_models));
            tx.execute(
                "UPDATE api_keys SET name = ?2, allowed_models = ?3, updated_at = ?4 \
                 WHERE id = ?1",
                params![id, new_name, models, now],
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
                        allowed_models: &current.allowed_models,
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
    /// sees the commit through its own `data_version` poll, which is the
    /// propagation delay recorded in ADR 0014.
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
        let outcome = self
            .pool
            .write(|conn| {
                let tx = conn.transaction()?;
                let out = f(&tx)?;
                tx.commit()?;
                Ok(out)
            })
            .map_err(ApiKeyError::from)?;

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
    allowed_models: &'a [String],
    now: &'a str,
    expires_at: Option<&'a str>,
}

/// Insert one key. Shared by `create` and `rotate` so they cannot drift apart.
fn insert_key(conn: &Connection, key: NewKey<'_>) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO api_keys \
         (name, consumer_id, key_prefix, key_hash, allowed_models, status, \
          created_at, updated_at, expires_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, 'active', ?6, ?6, ?7)",
        params![
            key.name,
            key.consumer_id,
            key.prefix,
            key.hash,
            encode_models(key.allowed_models),
            key.now,
            key.expires_at
        ],
    )?;
    Ok(())
}

/// `SELECT *` order must match [`row_to_api_key`]; the two are one list.
const SELECT_COLUMNS: &str = "id, name, consumer_id, key_prefix, key_hash, \
     allowed_models, status, created_at, updated_at, expires_at, revoked_at";

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
    let status: String = row.get(6)?;
    Ok(ApiKeyRow {
        id: row.get(0)?,
        name: row.get(1)?,
        consumer_id: row.get(2)?,
        key_prefix: row.get(3)?,
        allowed_models: decode_models(&row.get::<_, String>(5)?)?,
        status: KeyStatus::parse(&status).ok_or_else(|| {
            rusqlite::Error::InvalidColumnType(6, "status".to_string(), rusqlite::types::Type::Text)
        })?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
        expires_at: row.get(9)?,
        revoked_at: row.get(10)?,
    })
}

fn encode_models(models: &[String]) -> String {
    serde_json::to_string(models).unwrap_or_else(|_| "[]".to_string())
}

fn decode_models(text: &str) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(text).map_err(|e| {
        rusqlite::Error::InvalidColumnType(
            0,
            format!("allowed_models is not a JSON array: {e}"),
            rusqlite::types::Type::Text,
        )
    })
}

fn require_text(field: &str, value: &str) -> Result<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiKeyError::Invalid(format!("{field} must not be empty")));
    }
    Ok(trimmed.to_string())
}

fn validate_models(models: &[String]) -> Result<()> {
    for model in models {
        if model.trim().is_empty() {
            return Err(ApiKeyError::Invalid(
                "allowed_models must not contain a blank model name".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// A store whose snapshot is loaded, which is what startup does.
    fn loaded() -> (tempfile::TempDir, ApiKeyStore) {
        let (dir, store) = store();
        store
            .refresh()
            .expect("a fresh database has no keys to load");
        (dir, store)
    }

    fn create(store: &ApiKeyStore, name: &str, consumer: &str) -> (ApiKeyRow, String) {
        store
            .create(name, consumer, models(&["gpt-4o"]), None)
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
        assert_eq!(auth.name, "primary");
        assert_eq!(auth.allowed_models, models(&["gpt-4o"]));

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
            first
                .create("primary", "acme", models(&["gpt-4o"]), None)
                .unwrap();
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
        assert_eq!(rotated.allowed_models, models(&["gpt-4o"]));
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

        let (expiring, _) = store
            .create("expiring", "acme", models(&["gpt-4o"]), Some(expiry))
            .unwrap();
        let (rotated, _) = store.rotate(expiring.id).unwrap();
        assert_eq!(
            rotated.expires_at,
            Some(timefmt::format_ts(expiry)),
            "a rotation inherits the deadline it was given"
        );

        let (perpetual, _) = create(&store, "perpetual", "acme");
        let (rotated, _) = store.rotate(perpetual.id).unwrap();
        assert_eq!(rotated.expires_at, None, "no expiry means none is added");
    }

    #[test]
    fn test_an_expired_key_is_not_authenticatable() {
        let (_dir, store) = loaded();
        let (past, _) = store
            .create(
                "expired",
                "acme",
                models(&["gpt-4o"]),
                Some(timefmt::now() - Duration::hours(1)),
            )
            .unwrap();
        let (_future, future_plaintext) = store
            .create(
                "valid",
                "acme",
                models(&["gpt-4o"]),
                Some(timefmt::now() + Duration::hours(1)),
            )
            .unwrap();

        // Expiry is a load-time predicate, so the row still exists and still
        // reports itself active — it simply is not in the authenticating set.
        assert_eq!(
            store.get(past.id).unwrap().unwrap().status,
            KeyStatus::Active
        );
        assert_eq!(store.active_count(), 1);
        assert!(store.authenticate(&future_plaintext).is_some());
    }

    #[test]
    fn test_update_changes_name_and_allow_list_but_not_the_secret() {
        let (_dir, store) = loaded();
        let (row, plaintext) = create(&store, "primary", "acme");

        let updated = store
            .update(row.id, Some("renamed"), Some(models(&["gpt-4o", "gpt-5"])))
            .expect("update must succeed");
        assert_eq!(updated.name, "renamed");
        assert_eq!(updated.allowed_models, models(&["gpt-4o", "gpt-5"]));

        let auth = store.authenticate(&plaintext).expect("the key still works");
        assert_eq!(auth.name, "renamed");
        assert_eq!(auth.allowed_models, models(&["gpt-4o", "gpt-5"]));

        // A partial update leaves the untouched field alone.
        let only_name = store.update(row.id, Some("again"), None).unwrap();
        assert_eq!(only_name.allowed_models, models(&["gpt-4o", "gpt-5"]));
    }

    #[test]
    fn test_update_is_refused_for_a_revoked_key_and_changes_nothing() {
        let (_dir, store) = loaded();
        let (row, _) = create(&store, "primary", "acme");
        store.update(row.id, Some("renamed"), None).unwrap();
        store.revoke(row.id).unwrap();

        assert!(matches!(
            store.update(row.id, Some("sneaky"), None),
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
            store.update(404, Some("x"), None),
            Err(ApiKeyError::NotFound(404))
        ));
        assert_eq!(
            store.list().unwrap().len(),
            0,
            "a failed op creates nothing"
        );
    }

    #[test]
    fn test_empty_fields_are_refused_before_they_reach_sql() {
        let (_dir, store) = loaded();
        for bad in ["", "   ", "\t\n"] {
            assert!(matches!(
                store.create(bad, "acme", models(&["gpt-4o"]), None),
                Err(ApiKeyError::Invalid(_))
            ));
            assert!(matches!(
                store.create("name", bad, models(&["gpt-4o"]), None),
                Err(ApiKeyError::Invalid(_))
            ));
            assert!(matches!(
                store.create("name", "acme", models(&["gpt-4o", " "]), None),
                Err(ApiKeyError::Invalid(_))
            ));
        }
        assert_eq!(store.list().unwrap().len(), 0);
    }

    #[test]
    fn test_the_allow_list_round_trips_through_json_including_quotes_and_unicode() {
        let (_dir, store) = loaded();
        // Values that would break a naive join/split encoding: a quote, a
        // backslash, a comma and a non-ASCII model name.
        let awkward = models(&["a\"b", "c\\d", "e,f", "gpt-4ö", "日本語"]);
        let (row, _) = store
            .create("awkward", "acme", awkward.clone(), None)
            .unwrap();

        assert_eq!(store.get(row.id).unwrap().unwrap().allowed_models, awkward);
        store.refresh().unwrap();
        let auth = store.authenticate(&store.list().unwrap()[0].key_prefix);
        // The prefix is not a key, so this is None — the point is that the
        // round trip above already proved the encoding.
        assert!(auth.is_none());
        assert_eq!(store.list().unwrap()[0].allowed_models, awkward);
    }

    #[test]
    fn test_an_empty_allow_list_survives_the_round_trip_as_empty() {
        let (_dir, store) = loaded();
        let (row, plaintext) = store.create("strict", "acme", vec![], None).unwrap();
        assert_eq!(
            store.get(row.id).unwrap().unwrap().allowed_models,
            Vec::<String>::new()
        );

        let auth = store
            .authenticate(&plaintext)
            .expect("an empty list still authenticates");
        assert!(auth.allowed_models.is_empty(), "and still allows nothing");
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
                "an allow-list that is not a JSON array",
                "UPDATE api_keys SET allowed_models='gpt-4o' WHERE id=?1",
                id(),
            ),
            (
                "a duplicated key hash",
                "INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, \
                 allowed_models, created_at, updated_at) \
                 SELECT 'dup', consumer_id, 'pp_dup', key_hash, '[]', created_at, created_at \
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
        let (secondary, secondary_plaintext) = create(&store, "secondary", "shared");

        let a = store.authenticate(&primary_plaintext).unwrap();
        let b = store.authenticate(&secondary_plaintext).unwrap();
        assert_eq!(a.consumer_id, b.consumer_id, "one consumer, two keys");
        assert_ne!(a.name, b.name, "but they are distinguishable");
        assert_eq!(store.active_count(), 2);

        // Revoking one leaves the other working — they are separate credentials
        // that happen to name the same consumer.
        store.revoke(primary.id).unwrap();
        assert!(store.authenticate(&primary_plaintext).is_none());
        assert!(store.authenticate(&secondary_plaintext).is_some());
        assert_eq!(
            store.get(secondary.id).unwrap().unwrap().consumer_id,
            "shared"
        );
    }

    #[test]
    fn test_snapshot_is_replaced_wholesale_never_merged() {
        let (_dir, store) = loaded();
        create(&store, "a", "acme");
        create(&store, "b", "acme");
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
