mod block_cache;
mod compaction;
mod internal_key;
mod manifest;
mod sstable;

pub use block_cache::BlockCacheStats;
pub use compaction::{
    CompactionCandidate, CompactionPolicy, L0MergePolicy, LevelStrategy, TierStrategy,
};
pub use internal_key::{
    commit_ts_of, encode_internal_key, encode_internal_key_seq, matches_user_key, user_key_of,
    InternalKey, COMMIT_TS_LEN,
};

pub use manifest::{
    decode_manifest_edit, encode_manifest_edit, inspect_manifest_path, replay_manifest,
    DecodeEditResult, ManifestEdit, ManifestEditType, ManifestInspection, ManifestState,
    ManifestWarning, TableMetadata, CURRENT_FILE_NAME, CURRENT_TMP_FILE_NAME, MANIFEST_FILE_NAME,
    MANIFEST_HEADER_LEN, MANIFEST_MAGIC, MANIFEST_VERSION,
};
pub use sstable::{
    decode_footer, footer_stored_crc, fuzz_decode_data_block, fuzz_decode_index_block,
    inspect_sstable_path, SstEntry, SstFooter, SstInspection, SstableBuildOptions, SstableBuilder,
    SstableReader, COMPRESSION_CODEC_LZ4, COMPRESSION_CODEC_NONE, COMPRESSION_CODEC_ZSTD,
    SST_FOOTER_LEN, SST_FOOTER_LEN_V2, SST_FOOTER_LEN_V3, SST_MAGIC, SST_VERSION, SST_VERSION_V1,
    SST_VERSION_V2, SST_VERSION_V4,
};

use std::collections::BTreeMap;

use kaya_core::{Bytes, KeyValue, SequenceNumber};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueRecord {
    Put {
        value: Bytes,
        sequence: SequenceNumber,
    },
    Delete {
        sequence: SequenceNumber,
    },
}

impl ValueRecord {
    pub const fn sequence(&self) -> SequenceNumber {
        match self {
            Self::Put { sequence, .. } | Self::Delete { sequence } => *sequence,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueRecordRef<'a> {
    Put {
        value: &'a [u8],
        sequence: SequenceNumber,
    },
    Delete {
        sequence: SequenceNumber,
    },
}

/// In-memory multi-version map.
///
/// Map keys are typed [`InternalKey`] values ordered by (user_key ASC,
/// commit_ts DESC). `put` / `delete` take a user key and sequence and insert a
/// new version; existing versions of the same user key are retained. Same
/// internal key (same user key + sequence) replaces that version only.
#[derive(Debug, Clone, Default)]
pub struct Memtable {
    entries: BTreeMap<InternalKey, ValueRecord>,
    approximate_bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a Put version for `user_key` at `sequence` (commit_ts).
    pub fn put(&mut self, key: Bytes, value: Bytes, sequence: SequenceNumber) {
        let internal = InternalKey::from_seq(key, sequence);
        let old_size = self
            .entries
            .get(&internal)
            .map_or(0, |r| Self::entry_size(&internal, r));
        let new_rec = ValueRecord::Put { value, sequence };
        let new_size = Self::entry_size(&internal, &new_rec);
        self.entries.insert(internal, new_rec);
        self.approximate_bytes = self.approximate_bytes.saturating_sub(old_size) + new_size;
    }

    /// Insert a Delete (tombstone) version for `user_key` at `sequence`.
    pub fn delete(&mut self, key: Bytes, sequence: SequenceNumber) {
        let internal = InternalKey::from_seq(key, sequence);
        let old_size = self
            .entries
            .get(&internal)
            .map_or(0, |r| Self::entry_size(&internal, r));
        let new_rec = ValueRecord::Delete { sequence };
        let new_size = Self::entry_size(&internal, &new_rec);
        self.entries.insert(internal, new_rec);
        self.approximate_bytes = self.approximate_bytes.saturating_sub(old_size) + new_size;
    }

    /// Visible version at Latest (`read_ts = u64::MAX`).
    pub fn get(&self, key: &[u8]) -> Option<ValueRecordRef<'_>> {
        self.get_at(key, u64::MAX)
    }

    /// Newest version of `user_key` with `commit_ts <= read_ts`.
    pub fn get_at(&self, key: &[u8], read_ts: u64) -> Option<ValueRecordRef<'_>> {
        // With Ord (user ASC, ts DESC): versions with ts > read_ts sort before
        // InternalKey{key, read_ts}. range from that seek yields the first
        // entry with user_key == key and ts <= read_ts.
        let seek = InternalKey::new(key.to_vec(), read_ts);
        if let Some((ik, record)) = self.entries.range(seek..).next() {
            if ik.user_key.as_slice() != key {
                return None;
            }
            return Some(record_ref(record));
        }
        None
    }

    /// Latest-visible Puts under `prefix` — one row per user key (decoded).
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<KeyValue> {
        self.scan_prefix_at(prefix, u64::MAX)
    }

    /// Snapshot prefix scan: newest version with `commit_ts <= read_ts` per user
    /// key; only Puts are returned.
    pub fn scan_prefix_at(&self, prefix: &[u8], read_ts: u64) -> Vec<KeyValue> {
        let mut items = Vec::new();
        let mut current_uk: Option<Bytes> = None;
        let mut selected = false;

        for (ik, record) in Self::iter_prefix(&self.entries, prefix) {
            let uk = ik.user_key.as_slice();
            if !prefix.is_empty() && !uk.starts_with(prefix) {
                // Past the prefix range (user_key order is BTree order).
                break;
            }

            let is_new_user = current_uk.as_deref() != Some(uk);
            if is_new_user {
                current_uk = Some(uk.to_vec());
                selected = false;
            }
            if selected {
                continue;
            }

            if ik.commit_ts > read_ts {
                continue;
            }
            selected = true;
            if let ValueRecord::Put { value, .. } = record {
                items.push(KeyValue {
                    key: uk.to_vec(),
                    value: value.clone(),
                });
            }
        }
        items
    }

    /// All versions under prefix as `(user_key, Option<value>, sequence)`,
    /// sorted by (user_key ASC, commit_ts DESC). `None` value means deletion.
    pub fn raw_scan_prefix(&self, prefix: &[u8]) -> Vec<(Bytes, Option<Bytes>, SequenceNumber)> {
        let mut items = Vec::new();
        for (ik, record) in Self::iter_prefix(&self.entries, prefix) {
            let uk = ik.user_key.as_slice();
            if !prefix.is_empty() && !uk.starts_with(prefix) {
                break;
            }
            match record {
                ValueRecord::Put { value, sequence } => {
                    items.push((uk.to_vec(), Some(value.clone()), *sequence));
                }
                ValueRecord::Delete { sequence } => {
                    items.push((uk.to_vec(), None, *sequence));
                }
            }
        }
        items
    }

    /// Zero-copy iterator over all version entries (typed keys + records).
    /// Preferred for internal full-table processing (snapshot capture).
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &ValueRecord)> {
        self.entries.iter()
    }

    /// One `(user_key, &ValueRecord)` per user key at Latest visibility.
    ///
    /// Used by flush: SST v1–v3 still store user keys; multi-version remains
    /// queryable in the memtable until Task 3/4 flush all versions as SST v4.
    /// Emission order is user_key ascending (safe for SST builders).
    pub fn iter_latest_user(&self) -> impl Iterator<Item = (Bytes, &ValueRecord)> + '_ {
        let mut out: Vec<(Bytes, &ValueRecord)> = Vec::new();
        let mut last_uk: Option<&[u8]> = None;
        for (ik, rec) in &self.entries {
            let uk = ik.user_key.as_slice();
            if last_uk == Some(uk) {
                continue; // older version of same user key (ts DESC)
            }
            last_uk = Some(uk);
            out.push((uk.to_vec(), rec));
        }
        out.into_iter()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    pub fn freeze(self) -> ImmutableMemtable {
        ImmutableMemtable {
            entries: self.entries,
            approximate_bytes: self.approximate_bytes,
        }
    }

    /// Iterate entries possibly under a user-key prefix.
    ///
    /// For a non-empty prefix, seeks to the first key with `user_key >= prefix`
    /// (highest commit_ts for an exact prefix match). Caller must stop when
    /// `ik.user_key` no longer starts with `prefix`.
    fn iter_prefix<'a>(
        entries: &'a BTreeMap<InternalKey, ValueRecord>,
        prefix: &[u8],
    ) -> Box<dyn Iterator<Item = (&'a InternalKey, &'a ValueRecord)> + 'a> {
        if prefix.is_empty() {
            Box::new(entries.iter())
        } else {
            // Highest commit_ts for exact user_key == prefix is the earliest
            // map position among keys with user_key >= prefix.
            let start = InternalKey::new(prefix.to_vec(), u64::MAX);
            Box::new(entries.range(start..))
        }
    }

    /// Size accounting mirrors wire encode: user_key + 8-byte ts + value.
    fn entry_size(key: &InternalKey, record: &ValueRecord) -> usize {
        key.user_key.len()
            + COMMIT_TS_LEN
            + match record {
                ValueRecord::Put { value, .. } => value.len(),
                ValueRecord::Delete { .. } => 0,
            }
    }
}

fn record_ref(record: &ValueRecord) -> ValueRecordRef<'_> {
    match record {
        ValueRecord::Put { value, sequence } => ValueRecordRef::Put {
            value,
            sequence: *sequence,
        },
        ValueRecord::Delete { sequence } => ValueRecordRef::Delete {
            sequence: *sequence,
        },
    }
}

#[derive(Debug, Clone)]
pub struct ImmutableMemtable {
    entries: BTreeMap<InternalKey, ValueRecord>,
    approximate_bytes: usize,
}

impl ImmutableMemtable {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn approximate_bytes(&self) -> usize {
        self.approximate_bytes
    }

    /// Iterate all version entries including tombstones, in InternalKey order.
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &ValueRecord)> {
        self.entries.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delete_hides_put() {
        let mut memtable = Memtable::new();
        memtable.put(b"k".to_vec(), b"v".to_vec(), SequenceNumber::new(1));
        memtable.delete(b"k".to_vec(), SequenceNumber::new(2));
        assert!(matches!(
            memtable.get(b"k"),
            Some(ValueRecordRef::Delete { .. })
        ));
    }

    #[test]
    fn scan_prefix_returns_sorted_visible_puts() {
        let mut memtable = Memtable::new();
        memtable.put(b"user:2".to_vec(), b"b".to_vec(), SequenceNumber::new(1));
        memtable.put(b"user:1".to_vec(), b"a".to_vec(), SequenceNumber::new(2));
        memtable.delete(b"user:3".to_vec(), SequenceNumber::new(3));
        let items = memtable.scan_prefix(b"user:");
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].key, b"user:1");
        assert_eq!(items[1].key, b"user:2");
    }

    #[test]
    fn memtable_keeps_two_versions() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v1".to_vec(), SequenceNumber::new(1));
        m.put(b"k".to_vec(), b"v2".to_vec(), SequenceNumber::new(2));
        match m.get_at(b"k", 1) {
            Some(ValueRecordRef::Put { value, .. }) => assert_eq!(value, b"v1"),
            _ => panic!("expected v1 at ts=1"),
        }
        match m.get_at(b"k", 2) {
            Some(ValueRecordRef::Put { value, .. }) => assert_eq!(value, b"v2"),
            _ => panic!("expected v2 at ts=2"),
        }
        match m.get(b"k") {
            Some(ValueRecordRef::Put { value, .. }) => assert_eq!(value, b"v2"),
            _ => panic!("expected latest v2"),
        }
    }

    #[test]
    fn memtable_tombstone_hides_at_new_ts_keeps_old() {
        let mut m = Memtable::new();
        m.put(b"k".to_vec(), b"v1".to_vec(), SequenceNumber::new(1));
        m.delete(b"k".to_vec(), SequenceNumber::new(2));
        assert!(matches!(
            m.get_at(b"k", 2),
            Some(ValueRecordRef::Delete { .. })
        ));
        match m.get_at(b"k", 1) {
            Some(ValueRecordRef::Put { value, .. }) => assert_eq!(value, b"v1"),
            _ => panic!("expected v1 at ts=1"),
        }
    }

    #[test]
    fn approximate_bytes_is_incremental_and_accurate() {
        let mut m = Memtable::new();
        assert_eq!(m.approximate_bytes(), 0);

        m.put(b"abc".to_vec(), b"xyz".to_vec(), SequenceNumber::new(1));
        // internal key = user_key(3) + 8 ts bytes; value 3 → 14
        assert_eq!(m.approximate_bytes(), 3 + COMMIT_TS_LEN + 3);

        m.put(
            b"abc".to_vec(),
            b"longervalue".to_vec(),
            SequenceNumber::new(2),
        );
        // second version retained: + (3+8) + 11
        assert_eq!(
            m.approximate_bytes(),
            (3 + COMMIT_TS_LEN + 3) + (3 + COMMIT_TS_LEN + 11)
        );

        m.delete(b"abc".to_vec(), SequenceNumber::new(3));
        // third version (tombstone): + (3+8) + 0
        assert_eq!(
            m.approximate_bytes(),
            (3 + COMMIT_TS_LEN + 3) + (3 + COMMIT_TS_LEN + 11) + (3 + COMMIT_TS_LEN)
        );

        // another key
        m.put(b"def".to_vec(), vec![0u8; 100], SequenceNumber::new(4));
        assert_eq!(
            m.approximate_bytes(),
            (3 + COMMIT_TS_LEN + 3)
                + (3 + COMMIT_TS_LEN + 11)
                + (3 + COMMIT_TS_LEN)
                + (3 + COMMIT_TS_LEN + 100)
        );
    }

    #[test]
    fn prefix_user_keys_order_and_visibility() {
        let mut m = Memtable::new();
        m.put(b"user:1".to_vec(), b"v1".to_vec(), SequenceNumber::new(1));
        m.put(b"user".to_vec(), b"v0".to_vec(), SequenceNumber::new(2));
        // get both
        assert!(
            matches!(m.get(b"user"), Some(ValueRecordRef::Put { value, .. }) if value == b"v0")
        );
        assert!(
            matches!(m.get(b"user:1"), Some(ValueRecordRef::Put { value, .. }) if value == b"v1")
        );
        let scan = m.scan_prefix(b"user");
        // user then user:1 in user_key order
        assert_eq!(scan.len(), 2);
        assert_eq!(scan[0].key, b"user");
        assert_eq!(scan[1].key, b"user:1");

        // raw_scan also in user_key order
        let raw = m.raw_scan_prefix(b"user");
        assert_eq!(raw.len(), 2);
        assert_eq!(raw[0].0, b"user");
        assert_eq!(raw[1].0, b"user:1");
    }

    #[test]
    fn iter_latest_user_sorted_for_flush() {
        let mut m = Memtable::new();
        m.put(b"aa".to_vec(), b"2".to_vec(), SequenceNumber::new(1));
        m.put(b"a".to_vec(), b"1".to_vec(), SequenceNumber::new(2));
        m.put(b"a".to_vec(), b"1b".to_vec(), SequenceNumber::new(3));
        let keys: Vec<_> = m.iter_latest_user().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"aa".to_vec()]);
        match m.get(b"a") {
            Some(ValueRecordRef::Put { value, .. }) => assert_eq!(value, b"1b"),
            _ => panic!("expected latest put for a"),
        }
    }

    // KD-0503: malformed SSTable footer input must not panic.
    #[test]
    fn fuzz_sstable_footer_no_panic() {
        let cases: &[&[u8]] = &[
            b"",
            &[0u8; 1],
            &[0u8; 47],
            &[0u8; 48],
            &[0xffu8; 48],
            &[0u8; 100],
            &[0xffu8; 100],
            b"\x00\x01\x02\x03\x04\x05\x06\x07garbage",
            b"\x4b\x53\x53\x54\x01\x00\x30\x00garbage",
        ];
        for input in cases {
            let _ = decode_footer(input); // must not panic
        }
    }

    // KD-0503: malformed manifest edit input must not panic.
    #[test]
    fn fuzz_manifest_decoder_no_panic() {
        let cases: &[&[u8]] = &[
            b"",
            &[0u8; 1],
            &[0u8; 31],
            &[0u8; 32],
            &[0xffu8; 32],
            &[0u8; 100],
            &[0xffu8; 100],
            b"\x4b\x4d\x41\x4e\x01\x00\x20\x00garbage_after_magic",
            b"\x00\xff\x80\x7f\xde\xad\xbe\xef\xca\xfe\xba\xbe",
        ];
        for input in cases {
            let _ = decode_manifest_edit(input); // must not panic
        }
    }

    // Fuzz data/index block decoder input must not panic.
    #[test]
    fn fuzz_sstable_block_no_panic() {
        let cases: &[&[u8]] = &[
            b"",
            &[0u8; 1],
            &[0u8; 11],
            &[0u8; 12],
            &[0xffu8; 12],
            &[0u8; 100],
            &[0xffu8; 100],
            b"\x00\x01\x02\x03\x04\x05\x06\x07garbage",
            b"\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
        ];
        for input in cases {
            fuzz_decode_data_block(input);
            fuzz_decode_index_block(input);
        }
    }
}
