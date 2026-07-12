mod block_cache;
mod compaction;
mod internal_key;
mod manifest;
mod sstable;

pub use block_cache::BlockCacheStats;
pub use internal_key::{
    commit_ts_of, encode_internal_key, encode_internal_key_seq, matches_user_key, user_key_of,
    COMMIT_TS_LEN,
};
pub use compaction::{
    CompactionCandidate, CompactionPolicy, L0MergePolicy, LevelStrategy, TierStrategy,
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
    SST_VERSION_V2,
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

#[derive(Debug, Clone, Default)]
pub struct Memtable {
    entries: BTreeMap<Bytes, ValueRecord>,
    approximate_bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: Bytes, value: Bytes, sequence: SequenceNumber) {
        let old_size = self
            .entries
            .get(&key)
            .map_or(0, |r| Self::entry_size(&key, r));
        let new_rec = ValueRecord::Put { value, sequence };
        let new_size = Self::entry_size(&key, &new_rec);
        self.entries.insert(key, new_rec);
        self.approximate_bytes = self.approximate_bytes.saturating_sub(old_size) + new_size;
    }

    pub fn delete(&mut self, key: Bytes, sequence: SequenceNumber) {
        let old_size = self
            .entries
            .get(&key)
            .map_or(0, |r| Self::entry_size(&key, r));
        let new_rec = ValueRecord::Delete { sequence };
        let new_size = Self::entry_size(&key, &new_rec);
        self.entries.insert(key, new_rec);
        self.approximate_bytes = self.approximate_bytes.saturating_sub(old_size) + new_size;
    }

    pub fn get(&self, key: &[u8]) -> Option<ValueRecordRef<'_>> {
        self.entries.get(key).map(|record| match record {
            ValueRecord::Put { value, sequence } => ValueRecordRef::Put {
                value,
                sequence: *sequence,
            },
            ValueRecord::Delete { sequence } => ValueRecordRef::Delete {
                sequence: *sequence,
            },
        })
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<KeyValue> {
        let mut items = Vec::new();
        // Avoid bound allocation on full prefix scan (common internal + some user cases).
        if prefix.is_empty() {
            for (key, record) in &self.entries {
                if let ValueRecord::Put { value, .. } = record {
                    items.push(KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
            }
        } else {
            let start = prefix.to_vec();
            for (key, record) in self.entries.range(start..) {
                if !key.starts_with(prefix) {
                    break;
                }
                if let ValueRecord::Put { value, .. } = record {
                    items.push(KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
            }
        }
        items
    }

    /// Iterate all entries for a given prefix including tombstones.
    /// Returns `(key, Option<value>, sequence)` — `None` value means deletion.
    pub fn raw_scan_prefix(&self, prefix: &[u8]) -> Vec<(Bytes, Option<Bytes>, SequenceNumber)> {
        let mut items = Vec::new();
        // Avoid allocating the bound vec for the very common "dump everything" case (flush, snapshot).
        if prefix.is_empty() {
            for (key, record) in &self.entries {
                match record {
                    ValueRecord::Put { value, sequence } => {
                        items.push((key.clone(), Some(value.clone()), *sequence));
                    }
                    ValueRecord::Delete { sequence } => {
                        items.push((key.clone(), None, *sequence));
                    }
                }
            }
        } else {
            let start = prefix.to_vec();
            for (key, record) in self.entries.range(start..) {
                if !key.starts_with(prefix) {
                    break;
                }
                match record {
                    ValueRecord::Put { value, sequence } => {
                        items.push((key.clone(), Some(value.clone()), *sequence));
                    }
                    ValueRecord::Delete { sequence } => {
                        items.push((key.clone(), None, *sequence));
                    }
                }
            }
        }
        items
    }

    /// Zero-copy iterator over all entries (including tombstones).
    /// Preferred for internal full-table processing (flush, snapshot, create_snapshot)
    /// to avoid materializing a large intermediate Vec of owned tuples on every call.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &ValueRecord)> {
        self.entries.iter()
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

    fn entry_size(key: &[u8], record: &ValueRecord) -> usize {
        key.len()
            + match record {
                ValueRecord::Put { value, .. } => value.len(),
                ValueRecord::Delete { .. } => 0,
            }
    }
}

#[derive(Debug, Clone)]
pub struct ImmutableMemtable {
    entries: BTreeMap<Bytes, ValueRecord>,
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

    /// Iterate all entries including tombstones, in sorted key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &ValueRecord)> {
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
    fn approximate_bytes_is_incremental_and_accurate() {
        let mut m = Memtable::new();
        assert_eq!(m.approximate_bytes(), 0);

        m.put(b"abc".to_vec(), b"xyz".to_vec(), SequenceNumber::new(1));
        // key 3 + value 3
        assert_eq!(m.approximate_bytes(), 6);

        m.put(
            b"abc".to_vec(),
            b"longervalue".to_vec(),
            SequenceNumber::new(2),
        );
        // replace: old 6, new key3 + 11 = 14
        assert_eq!(m.approximate_bytes(), 14);

        m.delete(b"abc".to_vec(), SequenceNumber::new(3));
        // tombstone keeps only key size 3
        assert_eq!(m.approximate_bytes(), 3);

        // another key
        m.put(b"def".to_vec(), vec![0u8; 100], SequenceNumber::new(4));
        assert_eq!(m.approximate_bytes(), 3 + 3 + 100);
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
