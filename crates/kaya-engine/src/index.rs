//! Secondary indexes foundation (M18).
//!
//! MVP model: an index maps **full primary value → primary key** for user keys under a
//! configured `primary_prefix`. Index metadata and entries live under the reserved
//! system key space `\x00idx/…` and are maintained on the non-txn `put`/`delete` path
//! (and therefore also when `txn_commit` materializes intents via those paths).

use std::collections::BTreeMap;

use kaya_core::{Bytes, KayaError, Result};
use kaya_io::Disk;

use super::{Engine, ReadTimestamp, ScanOptions, WriteOptions, WriteResult};

/// Reserved system-key prefix for all index metadata and entries.
pub const INDEX_SYS_PREFIX: &[u8] = b"\x00idx/";
/// Metadata keys: `\x00idx/meta/{name}`.
pub const INDEX_META_PREFIX: &[u8] = b"\x00idx/meta/";
/// Data entry key prefix (binary layout follows; see encoding helpers).
pub const INDEX_DATA_PREFIX: &[u8] = b"\x00idx/data/";

const META_VERSION: u8 = 1;

/// In-memory / durable index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub name: String,
    /// Only keys that start with this prefix are indexed.
    pub primary_prefix: Bytes,
    /// Unique secondary values (always `false` in this foundation).
    pub unique: bool,
}

/// True if `key` is in the reserved index system space.
pub fn is_index_system_key(key: &[u8]) -> bool {
    key.starts_with(INDEX_SYS_PREFIX)
}

pub(crate) fn reject_if_system_key(key: &[u8]) -> Result<()> {
    if is_index_system_key(key) {
        return Err(KayaError::invalid_argument(
            "keys under reserved system prefix \\x00idx/ are not writable via public API",
        ));
    }
    Ok(())
}

fn validate_index_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > 64 {
        return Err(KayaError::invalid_argument(
            "index name must be 1..=64 bytes",
        ));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(KayaError::invalid_argument(
            "index name must be ASCII alphanumeric, '_' or '-'",
        ));
    }
    Ok(())
}

pub(crate) fn encode_meta_key(name: &str) -> Bytes {
    let mut k = INDEX_META_PREFIX.to_vec();
    k.extend_from_slice(name.as_bytes());
    k
}

fn name_from_meta_key(key: &[u8]) -> Option<&str> {
    let rest = key.strip_prefix(INDEX_META_PREFIX)?;
    std::str::from_utf8(rest).ok()
}

/// Binary meta value: version u8 | unique u8 | prefix_len u32be | primary_prefix.
pub(crate) fn encode_meta_value(def: &IndexDef) -> Bytes {
    let mut v = Vec::with_capacity(6 + def.primary_prefix.len());
    v.push(META_VERSION);
    v.push(u8::from(def.unique));
    v.extend_from_slice(&(def.primary_prefix.len() as u32).to_be_bytes());
    v.extend_from_slice(&def.primary_prefix);
    v
}

pub(crate) fn decode_meta_value(name: &str, value: &[u8]) -> Result<IndexDef> {
    if value.len() < 6 {
        return Err(KayaError::corruption("index meta value too short"));
    }
    let version = value[0];
    if version != META_VERSION {
        return Err(KayaError::corruption(format!(
            "unsupported index meta version {version}"
        )));
    }
    let unique = value[1] != 0;
    let prefix_len = u32::from_be_bytes(value[2..6].try_into().unwrap()) as usize;
    if value.len() != 6 + prefix_len {
        return Err(KayaError::corruption(
            "index meta value length mismatch",
        ));
    }
    Ok(IndexDef {
        name: name.to_string(),
        primary_prefix: value[6..].to_vec(),
        unique,
    })
}

/// Data key layout (prefix-scan friendly on secondary):
/// `DATA_PREFIX || u16be(name_len) || name || secondary || primary || u32be(primary_len)`
pub(crate) fn encode_data_key(name: &str, secondary: &[u8], primary: &[u8]) -> Bytes {
    let name_b = name.as_bytes();
    let mut k = Vec::with_capacity(
        INDEX_DATA_PREFIX.len() + 2 + name_b.len() + secondary.len() + primary.len() + 4,
    );
    k.extend_from_slice(INDEX_DATA_PREFIX);
    k.extend_from_slice(&(name_b.len() as u16).to_be_bytes());
    k.extend_from_slice(name_b);
    k.extend_from_slice(secondary);
    k.extend_from_slice(primary);
    k.extend_from_slice(&(primary.len() as u32).to_be_bytes());
    k
}

/// Prefix for `scan_by_index(name, value_prefix)`.
pub(crate) fn encode_data_scan_prefix(name: &str, value_prefix: &[u8]) -> Bytes {
    let name_b = name.as_bytes();
    let mut k = Vec::with_capacity(INDEX_DATA_PREFIX.len() + 2 + name_b.len() + value_prefix.len());
    k.extend_from_slice(INDEX_DATA_PREFIX);
    k.extend_from_slice(&(name_b.len() as u16).to_be_bytes());
    k.extend_from_slice(name_b);
    k.extend_from_slice(value_prefix);
    k
}

/// Decode a data key for a known index name into `(secondary, primary)`.
pub(crate) fn decode_data_key(name: &str, key: &[u8]) -> Option<(Bytes, Bytes)> {
    let rest = key.strip_prefix(INDEX_DATA_PREFIX)?;
    if rest.len() < 2 {
        return None;
    }
    let name_len = u16::from_be_bytes(rest[0..2].try_into().ok()?) as usize;
    if rest.len() < 2 + name_len + 4 {
        return None;
    }
    let key_name = std::str::from_utf8(&rest[2..2 + name_len]).ok()?;
    if key_name != name {
        return None;
    }
    let after_name = &rest[2 + name_len..];
    if after_name.len() < 4 {
        return None;
    }
    let pk_len = u32::from_be_bytes(after_name[after_name.len() - 4..].try_into().ok()?) as usize;
    if after_name.len() < 4 + pk_len {
        return None;
    }
    let body = &after_name[..after_name.len() - 4];
    let primary = body[body.len() - pk_len..].to_vec();
    let secondary = body[..body.len() - pk_len].to_vec();
    Some((secondary, primary))
}

impl<D: Disk> Engine<D> {
    /// Create a secondary index over keys with `primary_prefix`.
    ///
    /// Secondary key = full primary value (MVP extraction). Existing matching
    /// keys are backfilled best-effort in this call.
    pub async fn create_index(&mut self, name: &str, primary_prefix: &[u8]) -> Result<()> {
        validate_index_name(name)?;
        if self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "index {name:?} already exists"
            )));
        }
        // Reject empty prefix only as "all keys" if explicitly allowed — MVP requires
        // a non-empty primary prefix to avoid accidental full-space indexes.
        if primary_prefix.is_empty() {
            return Err(KayaError::invalid_argument(
                "primary_prefix must be non-empty in M18 foundation",
            ));
        }
        self.validate_key(primary_prefix)?;

        let def = IndexDef {
            name: name.to_string(),
            primary_prefix: primary_prefix.to_vec(),
            unique: false,
        };
        let meta_key = encode_meta_key(name);
        let meta_val = encode_meta_value(&def);
        let opts = WriteOptions::default();
        self.write_put(meta_key, meta_val, opts.clone()).await?;
        self.indexes.insert(name.to_string(), def);

        // Best-effort backfill of current latest values under the prefix.
        let existing = self.scan_prefix_inner(primary_prefix, ScanOptions::default())?;
        for kv in existing {
            if is_index_system_key(&kv.key) {
                continue;
            }
            self.insert_index_entries_for(&kv.key, &kv.value, &opts)
                .await?;
        }
        Ok(())
    }

    /// Registered index names (sorted).
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Drop index metadata and all data entries.
    pub async fn drop_index(&mut self, name: &str) -> Result<()> {
        if !self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "unknown index {name:?}"
            )));
        }
        let opts = WriteOptions::default();
        let scan_prefix = encode_data_scan_prefix(name, b"");
        let entries = self.scan_prefix_inner(&scan_prefix, ScanOptions::default())?;
        for kv in entries {
            // Only delete keys that decode as this index's data entries.
            if decode_data_key(name, &kv.key).is_some() {
                self.write_delete(kv.key, opts.clone()).await?;
            }
        }
        self.write_delete(encode_meta_key(name), opts).await?;
        self.indexes.remove(name);
        Ok(())
    }

    /// Scan index entries whose secondary value starts with `value_prefix`.
    ///
    /// Returns `(secondary_value, primary_key)` pairs in key order.
    pub async fn scan_by_index(
        &mut self,
        name: &str,
        value_prefix: &[u8],
    ) -> Result<Vec<(Bytes, Bytes)>> {
        if !self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "unknown index {name:?}"
            )));
        }
        let scan_prefix = encode_data_scan_prefix(name, value_prefix);
        let items = self.scan_prefix_inner(&scan_prefix, ScanOptions::default())?;
        let mut out = Vec::with_capacity(items.len());
        for kv in items {
            if let Some((sec, pk)) = decode_data_key(name, &kv.key) {
                // Enforce prefix match (scan_prefix is byte-prefix; always true when
                // encoding is correct, but guard against meta/data key collisions).
                if sec.starts_with(value_prefix) {
                    out.push((sec, pk));
                }
            }
        }
        Ok(out)
    }

    /// Load durable index metadata into `self.indexes` (called from `open`).
    pub(crate) fn load_index_metadata(&mut self) -> Result<()> {
        let items = self.scan_prefix_inner(INDEX_META_PREFIX, ScanOptions::default())?;
        let mut map = BTreeMap::new();
        for kv in items {
            let Some(name) = name_from_meta_key(&kv.key) else {
                continue;
            };
            let def = decode_meta_value(name, &kv.value)?;
            map.insert(def.name.clone(), def);
        }
        self.indexes = map;
        Ok(())
    }

    /// After a successful user put: update matching index entries.
    pub(crate) async fn maintain_indexes_after_put(
        &mut self,
        key: &[u8],
        old_value: Option<&[u8]>,
        new_value: &[u8],
        opts: &WriteOptions,
    ) -> Result<()> {
        if is_index_system_key(key) || self.indexes.is_empty() {
            return Ok(());
        }
        let matching: Vec<IndexDef> = self
            .indexes
            .values()
            .filter(|d| key.starts_with(&d.primary_prefix))
            .cloned()
            .collect();
        if matching.is_empty() {
            return Ok(());
        }

        if let Some(old) = old_value {
            if old != new_value {
                for def in &matching {
                    let old_ik = encode_data_key(&def.name, old, key);
                    self.write_delete(old_ik, opts.clone()).await?;
                }
            } else {
                // Value unchanged → index entry already correct (if present).
                // Still ensure entry exists (covers create-after-write edge).
                for def in &matching {
                    let ik = encode_data_key(&def.name, new_value, key);
                    if self.get_inner(&ik, ReadTimestamp::Latest)?.is_none() {
                        self.write_put(ik, Bytes::new(), opts.clone()).await?;
                    }
                }
                return Ok(());
            }
        }
        for def in &matching {
            let ik = encode_data_key(&def.name, new_value, key);
            self.write_put(ik, Bytes::new(), opts.clone()).await?;
        }
        Ok(())
    }

    /// After a successful user delete: remove matching index entries.
    pub(crate) async fn maintain_indexes_after_delete(
        &mut self,
        key: &[u8],
        old_value: Option<&[u8]>,
        opts: &WriteOptions,
    ) -> Result<()> {
        if is_index_system_key(key) || self.indexes.is_empty() {
            return Ok(());
        }
        let Some(old) = old_value else {
            return Ok(());
        };
        let matching: Vec<String> = self
            .indexes
            .values()
            .filter(|d| key.starts_with(&d.primary_prefix))
            .map(|d| d.name.clone())
            .collect();
        for name in matching {
            let ik = encode_data_key(&name, old, key);
            self.write_delete(ik, opts.clone()).await?;
        }
        Ok(())
    }

    async fn insert_index_entries_for(
        &mut self,
        key: &[u8],
        value: &[u8],
        opts: &WriteOptions,
    ) -> Result<()> {
        let matching: Vec<String> = self
            .indexes
            .values()
            .filter(|d| key.starts_with(&d.primary_prefix))
            .map(|d| d.name.clone())
            .collect();
        for name in matching {
            let ik = encode_data_key(&name, value, key);
            self.write_put(ik, Bytes::new(), opts.clone()).await?;
        }
        Ok(())
    }

    /// Raw put (WAL + memtable) without index maintenance or system-key rejection.
    pub(crate) async fn write_put(
        &mut self,
        key: Bytes,
        value: Bytes,
        opts: WriteOptions,
    ) -> Result<WriteResult> {
        self.validate_key(&key)?;
        self.validate_value(&value)?;
        self.prepare_hlc_write_sequence().await;
        let durability = opts.durability.unwrap_or(self.config.durability.mode);
        let append = self
            .wal
            .append(
                kaya_wal::WalPayload::Put {
                    key: key.clone(),
                    value: value.clone(),
                },
                durability,
            )
            .await?;
        self.memtable.put(key, value, append.sequence);
        self.stats.put_count += 1;
        self.stats.memtable_entries = self.memtable.len() as u64;
        self.stats.wal_bytes_written += u64::from(append.encoded_len);
        self.stats.wal_fsync_count += u64::from(append.durable);
        if let Some(us) = append.fsync_duration_us {
            self.stats.wal_fsync_total_us += us;
            if us > self.stats.wal_fsync_max_us {
                self.stats.wal_fsync_max_us = us;
            }
            self.histograms.wal_fsync_us.observe(us);
        }
        self.stats.last_sequence = append.sequence.get();
        self.maybe_auto_flush().await?;
        Ok(WriteResult {
            sequence: append.sequence,
            lsn: append.lsn,
            durable: append.durable,
        })
    }

    /// Raw delete (WAL + memtable) without index maintenance or system-key rejection.
    pub(crate) async fn write_delete(
        &mut self,
        key: Bytes,
        opts: WriteOptions,
    ) -> Result<WriteResult> {
        self.validate_key(&key)?;
        self.prepare_hlc_write_sequence().await;
        let durability = opts.durability.unwrap_or(self.config.durability.mode);
        let append = self
            .wal
            .append(kaya_wal::WalPayload::Delete { key: key.clone() }, durability)
            .await?;
        self.memtable.delete(key, append.sequence);
        self.stats.delete_count += 1;
        self.stats.memtable_entries = self.memtable.len() as u64;
        self.stats.wal_bytes_written += u64::from(append.encoded_len);
        self.stats.wal_fsync_count += u64::from(append.durable);
        if let Some(us) = append.fsync_duration_us {
            self.stats.wal_fsync_total_us += us;
            if us > self.stats.wal_fsync_max_us {
                self.stats.wal_fsync_max_us = us;
            }
            self.histograms.wal_fsync_us.observe(us);
        }
        self.stats.last_sequence = append.sequence.get();
        self.maybe_auto_flush().await?;
        Ok(WriteResult {
            sequence: append.sequence,
            lsn: append.lsn,
            durable: append.durable,
        })
    }
}

#[cfg(test)]
mod encoding_tests {
    use super::*;

    #[test]
    fn data_key_roundtrip() {
        let key = encode_data_key("by_email", b"a@x.com", b"user:1");
        let (sec, pk) = decode_data_key("by_email", &key).unwrap();
        assert_eq!(sec, b"a@x.com");
        assert_eq!(pk, b"user:1");
    }

    #[test]
    fn data_key_prefix_scan_order() {
        let a = encode_data_key("idx", b"alice", b"pk1");
        let b = encode_data_key("idx", b"bob", b"pk2");
        let p = encode_data_scan_prefix("idx", b"a");
        assert!(a.starts_with(&p));
        assert!(!b.starts_with(&p));
        assert!(a < b);
    }

    #[test]
    fn meta_value_roundtrip() {
        let def = IndexDef {
            name: "x".into(),
            primary_prefix: b"user:".to_vec(),
            unique: false,
        };
        let v = encode_meta_value(&def);
        let got = decode_meta_value("x", &v).unwrap();
        assert_eq!(got, def);
    }
}
