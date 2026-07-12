//! Internal key codec and in-memory key type for MVCC (M16).
//!
//! Wire layout (SST v4 / on-disk): `user_key ‖ (u64::MAX - commit_ts).to_be_bytes()`
//!
//! In-memory memtable order uses the typed [`InternalKey`] with a custom `Ord`
//! (user_key ASC, commit_ts DESC). Raw byte suffix encoding does **not** preserve
//! user_key lexicographic order when one user_key is a proper prefix of another
//! (e.g. `"user"` vs `"user:1"`), so memtable must not sort on encoded bytes.
//!
//! See `spec/docs/mvcc-spec.md`.

use std::cmp::Ordering;

use kaya_core::{Bytes, SequenceNumber};

/// Length of the inverted commit-ts suffix in bytes.
pub const COMMIT_TS_LEN: usize = 8;

/// Typed multi-version key used as the memtable map key.
///
/// Ordering: `user_key` ascending, then `commit_ts` descending (newest first).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Bytes,
    pub commit_ts: u64,
}

impl InternalKey {
    pub fn new(user_key: impl Into<Bytes>, commit_ts: u64) -> Self {
        Self {
            user_key: user_key.into(),
            commit_ts,
        }
    }

    pub fn from_seq(user_key: impl Into<Bytes>, seq: SequenceNumber) -> Self {
        Self::new(user_key, seq.get())
    }

    /// Encode to the on-disk / wire form (for SST v4 later).
    pub fn encode(&self) -> Bytes {
        encode_internal_key(&self.user_key, self.commit_ts)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.commit_ts.cmp(&self.commit_ts))
    }
}

/// Encode a versioned internal key: user_key + inverted big-endian commit_ts.
///
/// Kept for SST v4 / wire compatibility. Do **not** use raw encoded bytes as
/// BTree keys for memtable ordering — use [`InternalKey`] instead.
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

    /// Demonstrates the wire-encoding bug: raw suffix bytes do not preserve
    /// user_key order when one key is a proper prefix of another.
    #[test]
    fn wire_encode_breaks_proper_prefix_user_key_order() {
        let user = encode_internal_key(b"user", 1);
        let user_1 = encode_internal_key(b"user:1", 1);
        // user_key b"user" < b"user:1", but encoded order is reversed:
        assert!(user_1 < user);
        // Typed InternalKey preserves logical user_key order:
        let t_user = InternalKey::new(b"user".to_vec(), 1);
        let t_user_1 = InternalKey::new(b"user:1".to_vec(), 1);
        assert!(t_user < t_user_1);
    }

    #[test]
    fn typed_internal_key_orders_user_asc_ts_desc() {
        let a100 = InternalKey::new(b"a".to_vec(), 100);
        let a50 = InternalKey::new(b"a".to_vec(), 50);
        let b1 = InternalKey::new(b"b".to_vec(), 1);
        assert!(a100 < a50); // same user: higher ts first
        assert!(a50 < b1); // different user: user_key ASC
        assert!(a100 < b1);
    }

    #[test]
    fn typed_get_at_seek_lands_on_visible_version() {
        // For user K versions ts=100,50,10 and read_ts=60, seek (K,60) should
        // land so that the first entry >= seek is (K,50).
        let seek = InternalKey::new(b"k".to_vec(), 60);
        let v100 = InternalKey::new(b"k".to_vec(), 100);
        let v50 = InternalKey::new(b"k".to_vec(), 50);
        let v10 = InternalKey::new(b"k".to_vec(), 10);
        assert!(v100 < seek);
        assert!(seek <= v50);
        assert!(v50 < v10);
        let mut keys = [v100, v50.clone(), v10];
        keys.sort();
        let first_ge = keys.iter().find(|k| *k >= &seek).unwrap();
        assert_eq!(first_ge, &v50);
    }
}
