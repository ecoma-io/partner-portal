//! Small cryptographic primitives, hand-written over `sha2`.
//!
//! # Why this module exists
//!
//! Two unrelated features need the same three functions: the dashboard's opaque
//! pagination cursors ([`crate::dashboard::api`]) and partner API-key hashing
//! ([`crate::apikeys`]). They were written once in the dashboard and are moved
//! here rather than duplicated, so there is one HMAC-SHA256 in the tree and one
//! set of RFC test vectors proving it.
//!
//! # Why hand-written
//!
//! `sha2` is already a dependency and these are the constructions below — an
//! HMAC is two padded hashes. `hmac` and `subtle` would each be a dependency
//! replacing a dozen readable lines, and `AGENTS.md` asks for a reason before any
//! dependency is added. `constant_time_eq` is included for the cursor path; the
//! API-key path deliberately does **not** need it, because a lookup there is a
//! hash-map hit on a hex digest rather than a byte comparison of secrets.

use sha2::{Digest, Sha256};

/// Output length of SHA-256, in bytes.
pub const SHA256_OUTPUT: usize = 32;
/// Input block size of SHA-256, used to pad the HMAC key.
pub const SHA256_BLOCK: usize = 64;

/// URL-safe base64 alphabet (RFC 4648 §5), unpadded.
const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// HMAC-SHA256 (RFC 2104).
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; SHA256_OUTPUT] {
    // A key longer than the block size is hashed down first; anything shorter is
    // zero-padded to it.
    let mut block = [0u8; SHA256_BLOCK];
    if key.len() > SHA256_BLOCK {
        block[..SHA256_OUTPUT].copy_from_slice(&Sha256::digest(key)[..]);
    } else {
        block[..key.len()].copy_from_slice(key);
    }

    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(message);
    let inner = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner);
    let mut mac = [0u8; SHA256_OUTPUT];
    mac.copy_from_slice(&outer.finalize());
    mac
}

/// Compare two byte strings without an early exit, so a forger learns nothing
/// from how far the comparison got.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Encode bytes as unpadded URL-safe base64.
///
/// Hand-written on purpose: it is a dozen readable lines, the alphabet is the
/// whole format, and the alternative is a dependency for one function.
pub fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        // Read the group as one 24-bit big-endian number, zero-padded on the
        // right; a group of 1 or 2 bytes then simply emits fewer sextets.
        let mut padded = [0u8; 3];
        padded[..chunk.len()].copy_from_slice(chunk);
        let n = u32::from_be_bytes([0, padded[0], padded[1], padded[2]]);

        for (index, shift) in [18u32, 12, 6, 0].into_iter().enumerate() {
            if index < chunk.len() + 1 {
                out.push(B64_ALPHABET[((n >> shift) & 0b11_1111) as usize] as char);
            }
        }
    }
    out
}

/// Decode unpadded URL-safe base64.
///
/// Returns `None` for anything that is not a canonical encoding: a stray `=`,
/// a character outside the alphabet, a length that cannot carry whole bytes, or
/// a final sextet with non-zero padding bits.
pub fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3 + 2);
    let mut accumulator: u32 = 0;
    let mut bits: u32 = 0;

    for byte in text.bytes() {
        let value = B64_ALPHABET.iter().position(|&a| a == byte)? as u32;
        accumulator = (accumulator << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }

    // At most 4 leftover bits are meaningful; 6 or more means the last character
    // contributed no byte at all, which no encoder produces. Whatever is left
    // must be zero, or the encoding is non-canonical.
    if bits >= 6 || (accumulator & ((1 << bits) - 1)) != 0 {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base64url_matches_rfc4648_vectors() {
        for (raw, encoded) in [
            ("", ""),
            ("f", "Zg"),
            ("fo", "Zm8"),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg"),
            ("fooba", "Zm9vYmE"),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64url_encode(raw.as_bytes()), encoded);
            assert_eq!(base64url_decode(encoded).unwrap(), raw.as_bytes());
        }

        // The two characters that distinguish the URL-safe alphabet from the
        // standard one: 0xfb 0xff 0xbf encodes to "-_-_" here, "+/+/" there.
        let bytes = [0xfb, 0xff, 0xbf];
        assert_eq!(base64url_encode(&bytes), "-_-_");
    }

    #[test]
    fn test_base64url_round_trips_every_byte_value() {
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(base64url_decode(&base64url_encode(&all)).unwrap(), all);

        // Every length modulo 3, so the shortened final group is covered too.
        for len in 0..=all.len() {
            let slice = &all[..len];
            assert_eq!(
                base64url_decode(&base64url_encode(slice)).unwrap(),
                slice,
                "round trip failed at length {len}"
            );
        }
    }

    #[test]
    fn test_base64url_rejects_non_canonical_input() {
        for bad in [
            // Padding is never emitted, so it is never accepted.
            "=", "Zg=", "Zm9v=", "Zg==", "Zm9vYg==",
            // A length that cannot carry a whole byte, or characters outside
            // the alphabet (including the standard-alphabet `+` and `/`).
            "a", "!!!!", "Zm9v\n", "Zm+v", "Zm/v",
            // Non-zero padding bits: "Zh" and "ab" encode the same bytes as
            // "Zg" and "aa", which is what a canonical encoder emits.
            "Zh", "ab", "abd",
        ] {
            assert!(
                base64url_decode(bad).is_none(),
                "{bad:?} must not decode as unpadded URL-safe base64"
            );
        }

        // The canonical spellings of the same bytes do decode, so the rejections
        // above are about canonical form rather than the length alone.
        assert_eq!(base64url_decode("Zg").unwrap(), b"f");
        assert_eq!(base64url_decode("abc").unwrap(), b"i\xb7");
    }

    #[test]
    fn test_hmac_sha256_matches_rfc4231_vectors() {
        // RFC 4231 test case 2: key "Jefe", data "what do ya want for nothing?".
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );

        // RFC 4231 test case 1, which also exercises a key of exactly the block
        // size and the case below it.
        assert_eq!(
            hex::encode(hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );

        // RFC 4231 test case 6: a key longer than the block size, which must be
        // hashed down first.
        assert_eq!(
            hex::encode(hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn test_constant_time_eq_matches_equality() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        // Different lengths are unequal without reading past the end.
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(!constant_time_eq(b"ab", b"abc"));
    }
}
