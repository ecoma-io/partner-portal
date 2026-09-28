//! Partner API keys: hashing, generation, and the hashing secret.
//!
//! # What this module is
//!
//! The primitives that turn a partner's plaintext key into the value stored in
//! `api_keys.key_hash`, and back. It deliberately contains **no** database code
//! (that is [`store`]) and **no** HTTP (that is [`crate::admin::keys`]) — only
//! the two functions that decide whether a presented credential is the one that
//! was registered, and the secret both sides are keyed by.
//!
//! The two database-facing pieces are [`store`], which owns the table and the
//! in-memory snapshot authentication reads, and [`refresher`], which brings that
//! snapshot up to date when a *sibling* instance commits.
//!
//! # Why HMAC and not a bare SHA-256
//!
//! A bare digest of a high-entropy random key is not brute-forceable either —
//! the plaintext carries 256 bits of randomness. But the *stored* hash is then
//! the credential: anyone who reads the database can authenticate as any partner
//! forever, with no way to notice and nothing to revoke except the key itself.
//! Keying the digest with a secret that never touches the database changes that.
//! A stolen `api_keys` table then yields digests that are useless without
//! `PARTNER_PORTAL_API_KEY_SECRET`, which lives in the deployment's
//! environment and is the thing an operator rotates.
//!
//! The trade-off is real and is recorded in ADR 0014: **rotating the secret
//! invalidates every key**, because there is no plaintext to re-hash. That is
//! why the secret is a long-lived deployment secret rather than a per-key salt.

pub mod refresher;
pub mod store;

use std::io::Read;

use crate::crypto::{base64url_encode, hmac_sha256};

pub use refresher::{ApiKeyRefresher, StartupError};
pub use store::{ApiKeyAuth, ApiKeyError, ApiKeyRow, ApiKeySnapshot, ApiKeyStore};

/// Environment variable holding the secret that keys every API-key hash.
pub const SECRET_ENV: &str = "PARTNER_PORTAL_API_KEY_SECRET";

/// Minimum length of [`SECRET_ENV`], in bytes.
///
/// Not a cryptographic requirement — HMAC accepts any key — but a guard against
/// the failure that matters in practice: an operator types a short passphrase
/// where a generated value was intended, and the database's protection silently
/// rests on four words.
pub const MIN_SECRET_LEN: usize = 32;

/// Prefix every generated key carries, so one is recognisable in a log or a
/// support ticket. Not a secret: it is a constant, published in the source.
pub const KEY_PREFIX: &str = "pp_";

/// How much of a plaintext is stored as `key_prefix`.
///
/// Long enough to tell two keys apart in a list, short enough that it cannot be
/// brute-forced from — it carries no entropy the rest of the key does not.
pub const KEY_PREFIX_LEN: usize = 12;

/// Number of random bytes in a generated key.
const KEY_ENTROPY_BYTES: usize = 32;

/// Why the HMAC secret could not be loaded.
///
/// Carries the variable name and the length it was given, never the value: this
/// error reaches a log and a console.
#[derive(Debug)]
pub enum SecretError {
    /// The variable is not set, or set to the empty string.
    Missing,
    /// The value is set but too short to be treated as a generated secret.
    TooShort { len: usize },
}

impl std::fmt::Display for SecretError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecretError::Missing => write!(
                f,
                "{SECRET_ENV} is not set. It keys the hash of every partner API \
                 key and must be supplied by the deployment; without it no key \
                 can be issued or verified"
            ),
            SecretError::TooShort { len } => write!(
                f,
                "{SECRET_ENV} is {len} bytes; at least {MIN_SECRET_LEN} are \
                 required"
            ),
        }
    }
}

impl std::error::Error for SecretError {}

/// The hash a plaintext key is stored and looked up as.
///
/// Hex, so it is a plain TEXT column, greppable by an operator debugging a
/// deployment, and comparable byte-for-byte in a UNIQUE constraint.
pub fn derive_key_hash(secret: &[u8], plaintext: &str) -> String {
    hex::encode(hmac_sha256(secret, plaintext.as_bytes()))
}

/// The identifying head of a plaintext, stored beside the hash so an operator
/// can tell two keys apart without either one being recoverable.
pub fn key_prefix_of(plaintext: &str) -> String {
    plaintext.chars().take(KEY_PREFIX_LEN).collect()
}

/// Read and validate the hashing secret from the environment.
///
/// An unset variable and an empty one are the same failure to an operator — no
/// secret — so they report the same thing. The message names the variable, never
/// its value.
pub fn load_secret() -> Result<Vec<u8>, SecretError> {
    let value = std::env::var(SECRET_ENV).unwrap_or_default();
    if value.is_empty() {
        return Err(SecretError::Missing);
    }
    let bytes = value.into_bytes();
    if bytes.len() < MIN_SECRET_LEN {
        return Err(SecretError::TooShort { len: bytes.len() });
    }
    Ok(bytes)
}

/// Generate a new plaintext key: [`KEY_PREFIX`] plus 256 bits of OS entropy,
/// URL-safe base64.
///
/// # Why `/dev/urandom` and not a crate
///
/// `AGENTS.md` asks for a reason before a dependency is added, and the reason
/// here is not strong: this is one `read` on one file. A `rand` or `getrandom`
/// dependency would be a crate for a single call, and `rand`'s historical
/// seeding bugs are the wrong risk profile for the function that mints
/// credentials.
///
/// The failure mode is an error, never a weaker fallback. A key generator that
/// silently degraded when `/dev/urandom` was unavailable would be a silent
/// credential weakness, which is precisely the class of bug the repository's
/// "do not add a fallback that hides a failure" rule exists to prevent.
pub fn generate_plaintext() -> std::io::Result<String> {
    let mut bytes = [0u8; KEY_ENTROPY_BYTES];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(format!("{KEY_PREFIX}{}", base64url_encode(&bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &[u8] = b"a-test-secret-of-at-least-32-bytes!!";

    #[test]
    fn test_hash_is_hex_of_32_bytes() {
        let hash = derive_key_hash(SECRET, "pp_example");
        assert_eq!(hash.len(), 64, "SHA-256 is 32 bytes, hex doubles it");
        assert!(hash.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(hash, hash.to_lowercase(), "hex output is lower-case");
    }

    #[test]
    fn test_hash_is_deterministic_and_secret_dependent() {
        let key = "pp_example";
        // The two properties that make this a keyed hash rather than a digest:
        // the same input gives the same value, and a different key does not.
        assert_eq!(derive_key_hash(SECRET, key), derive_key_hash(SECRET, key));
        assert_ne!(
            derive_key_hash(SECRET, key),
            derive_key_hash(b"a-different-secret-of-32-bytes-!!!!", key)
        );
    }

    #[test]
    fn test_a_distinct_secret_cannot_verify_a_stored_hash() {
        // The property that motivates the whole design: digests lifted from the
        // database authenticate nothing on their own.
        let stored = derive_key_hash(SECRET, "pp_example");
        let attacker = b"an-attacker-guess-of-32-bytes-abcdef";
        assert_ne!(derive_key_hash(attacker, "pp_example"), stored);
    }

    #[test]
    fn test_key_prefix_is_not_the_key() {
        let plaintext = generate_plaintext().unwrap();
        let prefix = key_prefix_of(&plaintext);
        assert_eq!(prefix.len(), KEY_PREFIX_LEN);
        assert!(plaintext.starts_with(KEY_PREFIX));
        assert!(prefix.starts_with(KEY_PREFIX));
        // The prefix is a strict *head* — much shorter than the key it names,
        // and never the whole of it, so it carries no way to rebuild the secret.
        assert!(plaintext.starts_with(&prefix));
        assert!(plaintext.len() > prefix.len() + 16, "got {plaintext}");
    }

    #[test]
    fn test_generated_keys_are_distinct_and_well_formed() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..64 {
            let key = generate_plaintext().expect("/dev/urandom must be readable");
            assert!(key.starts_with(KEY_PREFIX), "got {key}");
            // Unpadded URL-safe base64 only: no '+', '/', '=' or whitespace,
            // because a key travels in an Authorization header.
            assert!(
                key[KEY_PREFIX.len()..]
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
                "got {key}"
            );
            // 32 bytes of entropy in 43 base64 characters.
            assert_eq!(key.len(), KEY_PREFIX.len() + 43, "got {key}");
            assert!(seen.insert(key), "generated keys must not repeat");
        }
    }

    #[test]
    fn test_secret_must_be_present_and_long_enough() {
        // `load_secret` reads the process environment, so the variable is set
        // and restored around the assertions rather than being mocked.
        let previous = std::env::var(SECRET_ENV).ok();

        // SAFETY: single-threaded within this test; the only other reader is
        // another `load_secret` call, which this test does not race.
        unsafe { std::env::set_var(SECRET_ENV, "") };
        assert!(matches!(load_secret(), Err(SecretError::Missing)));

        unsafe { std::env::set_var(SECRET_ENV, "too-short") };
        assert!(matches!(
            load_secret(),
            Err(SecretError::TooShort { len }) if len == 9
        ));

        let good = "x".repeat(MIN_SECRET_LEN);
        unsafe { std::env::set_var(SECRET_ENV, &good) };
        assert_eq!(load_secret().unwrap().len(), MIN_SECRET_LEN);

        // Exactly one byte short is rejected, not just obviously short values.
        unsafe { std::env::set_var(SECRET_ENV, "x".repeat(MIN_SECRET_LEN - 1)) };
        assert!(matches!(load_secret(), Err(SecretError::TooShort { .. })));

        // Trailing whitespace is part of the secret, not a delimiter: a value
        // that only reaches the minimum once spaces are counted is still a
        // value an operator chose, and trimming it here would make the stored
        // hashes depend on a rule nothing else in the product follows.
        unsafe { std::env::set_var(SECRET_ENV, format!("  {good}  ")) };
        assert_eq!(load_secret().unwrap(), format!("  {good}  ").into_bytes());

        match previous {
            Some(value) => unsafe { std::env::set_var(SECRET_ENV, value) },
            None => unsafe { std::env::remove_var(SECRET_ENV) },
        }
    }

    #[test]
    fn test_secret_errors_never_reveal_the_value() {
        let short = "a-short-secret";
        let rendered = SecretError::TooShort { len: short.len() }.to_string();
        assert!(!rendered.contains(short), "the value must not appear");
        assert!(rendered.contains(SECRET_ENV), "the variable name must");
    }
}
