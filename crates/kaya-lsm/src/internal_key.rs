//! Internal key codec for MVCC (M16).
//!
//! Layout: `user_key ‖ (u64::MAX - commit_ts).to_be_bytes()`
//! so BTree / SSTable order is user_key ASC, newest commit_ts first.
//!
//! See `spec/docs/mvcc-spec.md`.

use kaya_core::{Bytes, SequenceNumber};

/// Length of the inverted commit-ts suffix in bytes.
pub const COMMIT_TS_LEN: usize = 8;

/// Encode a versioned internal key: user_key + inverted big-endian commit_ts.
pub fn encode_internal_key(user_key: &[u8], commit_ts: u64) -> Bytes {
    let mut out = Vec::with_capacity(user_key.len() + COMMIT_TS_LEN);
    out.extend_from_slice(user_key);
    out.extend_from_slice(&(u64::MAX - commit_ts).to_be_bytes());
    out
}

/// Encode using a [`SequenceNumber`] as commit_ts (M16: commit_ts == seq).
pub fn encode_internal_key_seq(user_key: &[u8], seq: SequenceNumber) -> Bytes {
    encode_internal_key(user_key, seq.get())
}

/// Extract the user_key prefix from an internal key.
///
/// Keys shorter than [`COMMIT_TS_LEN`] are treated as legacy user_keys with no
/// inverted-ts suffix (dual-read helper).
pub fn user_key_of(internal_key: &[u8]) -> &[u8] {
    if internal_key.len() < COMMIT_TS_LEN {
        return internal_key;
    }
    &internal_key[..internal_key.len() - COMMIT_TS_LEN]
}

/// Decode commit_ts from an internal key suffix.
///
/// Returns `0` for keys shorter than [`COMMIT_TS_LEN`].
pub fn commit_ts_of(internal_key: &[u8]) -> u64 {
    if internal_key.len() < COMMIT_TS_LEN {
        return 0;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&internal_key[internal_key.len() - COMMIT_TS_LEN..]);
    u64::MAX - u64::from_be_bytes(buf)
}

/// True if `internal_key` is a version of exactly `user_key`.
pub fn matches_user_key(internal_key: &[u8], user_key: &[u8]) -> bool {
    user_key_of(internal_key) == user_key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let ik = encode_internal_key(b"abc", 42);
        assert_eq!(user_key_of(&ik), b"abc");
        assert_eq!(commit_ts_of(&ik), 42);
    }

    #[test]
    fn newer_ts_sorts_before_older_for_same_user_key() {
        let a = encode_internal_key(b"k", 10);
        let b = encode_internal_key(b"k", 20);
        assert!(b < a); // descending ts in key order
    }

    #[test]
    fn different_user_keys_order_by_user_key() {
        let a = encode_internal_key(b"a", 1);
        let b = encode_internal_key(b"b", 999);
        assert!(a < b);
    }

    #[test]
    fn user_key_prefix_bound() {
        let k = encode_internal_key(b"user", 5);
        assert!(user_key_of(&k).starts_with(b"us"));
    }

    #[test]
    fn encode_internal_key_seq_matches_get() {
        let seq = SequenceNumber::new(7);
        let ik = encode_internal_key_seq(b"x", seq);
        assert_eq!(user_key_of(&ik), b"x");
        assert_eq!(commit_ts_of(&ik), 7);
        assert_eq!(ik, encode_internal_key(b"x", 7));
    }

    #[test]
    fn matches_user_key_exact() {
        let ik = encode_internal_key(b"abc", 1);
        assert!(matches_user_key(&ik, b"abc"));
        assert!(!matches_user_key(&ik, b"ab"));
        assert!(!matches_user_key(&ik, b"abcd"));
    }

    #[test]
    fn short_legacy_key_helpers() {
        let short = b"hi";
        assert_eq!(user_key_of(short), b"hi");
        assert_eq!(commit_ts_of(short), 0);
        assert!(matches_user_key(short, b"hi"));
    }

    #[test]
    fn empty_user_key_roundtrip() {
        let ik = encode_internal_key(b"", 100);
        assert_eq!(user_key_of(&ik), b"");
        assert_eq!(commit_ts_of(&ik), 100);
        assert_eq!(ik.len(), COMMIT_TS_LEN);
    }

    #[test]
    fn max_commit_ts_roundtrip() {
        let ik = encode_internal_key(b"k", u64::MAX);
        assert_eq!(commit_ts_of(&ik), u64::MAX);
        assert_eq!(user_key_of(&ik), b"k");
    }
}
