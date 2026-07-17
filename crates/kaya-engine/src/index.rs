//! Secondary indexes (M18 + polish).
//!
//! An index maps **extracted secondary key → primary key** for user keys under a
//! configured `primary_prefix`. Index metadata and entries live under the reserved
//! system key space `\x00idx/…` and are maintained on the non-txn `put`/`delete` path
//! (and therefore also when `txn_commit` materializes intents via those paths).
//!
//! Polish (beyond foundation): field extractors, online backfill pause/resume,
//! `verify_index` divergence checker.

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

const META_VERSION_V1: u8 = 1;
const META_VERSION_V2: u8 = 2;

/// How the secondary key is derived from a primary value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum IndexExtractor {
    /// Secondary key = full primary value (default / foundation).
    #[default]
    WholeValue,
    /// First `len` bytes of the value (or the whole value if shorter).
    Prefix { len: u16 },
    /// Split value by `delimiter`, take 0-based field `index`.
    /// Missing field → no secondary (entry skipped / removed).
    Field { delimiter: u8, index: u16 },
}

impl IndexExtractor {
    /// Extract the secondary key from a primary value, if present.
    pub fn extract(&self, value: &[u8]) -> Option<Bytes> {
        match self {
            Self::WholeValue => Some(value.to_vec()),
            Self::Prefix { len } => {
                let n = (*len as usize).min(value.len());
                Some(value[..n].to_vec())
            }
            Self::Field { delimiter, index } => {
                let mut start = 0usize;
                let mut field_i = 0u16;
                for (i, &b) in value.iter().enumerate() {
                    if b == *delimiter {
                        if field_i == *index {
                            return Some(value[start..i].to_vec());
                        }
                        field_i = field_i.saturating_add(1);
                        start = i + 1;
                    }
                }
                if field_i == *index {
                    Some(value[start..].to_vec())
                } else {
                    None
                }
            }
        }
    }

    fn tag(&self) -> u8 {
        match self {
            Self::WholeValue => 0,
            Self::Prefix { .. } => 1,
            Self::Field { .. } => 2,
        }
    }

    fn encode_params(&self) -> Vec<u8> {
        match self {
            Self::WholeValue => Vec::new(),
            Self::Prefix { len } => len.to_be_bytes().to_vec(),
            Self::Field { delimiter, index } => {
                let mut v = Vec::with_capacity(3);
                v.push(*delimiter);
                v.extend_from_slice(&index.to_be_bytes());
                v
            }
        }
    }

    fn decode(tag: u8, params: &[u8]) -> Result<Self> {
        match tag {
            0 => Ok(Self::WholeValue),
            1 => {
                if params.len() != 2 {
                    return Err(KayaError::corruption("index extractor prefix params"));
                }
                Ok(Self::Prefix {
                    len: u16::from_be_bytes(params.try_into().unwrap()),
                })
            }
            2 => {
                if params.len() != 3 {
                    return Err(KayaError::corruption("index extractor field params"));
                }
                Ok(Self::Field {
                    delimiter: params[0],
                    index: u16::from_be_bytes(params[1..3].try_into().unwrap()),
                })
            }
            other => Err(KayaError::corruption(format!(
                "unknown index extractor tag {other}"
            ))),
        }
    }
}

/// Online backfill status for an index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackfillStatus {
    /// No online backfill in progress (sync create finished, or idle).
    Idle,
    /// Backfill running; more steps available.
    Running,
    /// Backfill paused by operator; resume continues from cursor.
    Paused,
    /// Online backfill completed.
    Complete,
}

/// Progress of online index backfill.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillProgress {
    pub status: BackfillStatus,
    pub scanned: u64,
    pub indexed: u64,
    /// Exclusive lower bound for the next scan batch (`None` = start).
    pub last_key: Option<Bytes>,
}

impl Default for BackfillProgress {
    fn default() -> Self {
        Self {
            status: BackfillStatus::Idle,
            scanned: 0,
            indexed: 0,
            last_key: None,
        }
    }
}

/// How `create_index` performs the initial backfill.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackfillMode {
    /// Scan and index all matching keys before returning (foundation behaviour).
    #[default]
    Sync,
    /// Register the index immediately; operator drives `index_backfill_step`.
    Online,
}

/// Options for creating a secondary index.
#[derive(Debug, Clone)]
pub struct CreateIndexOptions {
    pub extractor: IndexExtractor,
    pub backfill: BackfillMode,
}

impl Default for CreateIndexOptions {
    fn default() -> Self {
        Self {
            extractor: IndexExtractor::WholeValue,
            backfill: BackfillMode::Sync,
        }
    }
}

/// Kind of index↔primary divergence found by [`Engine::verify_index`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexDivergenceKind {
    /// Primary key has a value that should be indexed but no entry exists.
    MissingInIndex { expected_secondary: Bytes },
    /// Index entry exists but primary is missing or value no longer extracts to it.
    ExtraInIndex { secondary: Bytes },
}

/// A single divergence report from [`Engine::verify_index`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDivergence {
    pub primary_key: Bytes,
    pub kind: IndexDivergenceKind,
}

/// In-memory / durable index definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    pub name: String,
    /// Only keys that start with this prefix are indexed.
    pub primary_prefix: Bytes,
    /// Unique secondary values (always `false` in this foundation).
    pub unique: bool,
    /// How secondary keys are extracted from primary values.
    pub extractor: IndexExtractor,
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
    if crate::txn2pc::is_txn_system_key(key) {
        return Err(KayaError::invalid_argument(
            "keys under reserved system prefix \\x00txn/ are not writable via public API",
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

/// Binary meta value v2:
/// `version u8 | unique u8 | extractor_tag u8 | params_len u16be | params | prefix_len u32be | primary_prefix`
///
/// v1 (legacy): `version=1 | unique | prefix_len u32be | primary_prefix` → WholeValue.
pub(crate) fn encode_meta_value(def: &IndexDef) -> Bytes {
    let params = def.extractor.encode_params();
    let mut v = Vec::with_capacity(1 + 1 + 1 + 2 + params.len() + 4 + def.primary_prefix.len());
    v.push(META_VERSION_V2);
    v.push(u8::from(def.unique));
    v.push(def.extractor.tag());
    v.extend_from_slice(&(params.len() as u16).to_be_bytes());
    v.extend_from_slice(&params);
    v.extend_from_slice(&(def.primary_prefix.len() as u32).to_be_bytes());
    v.extend_from_slice(&def.primary_prefix);
    v
}

pub(crate) fn decode_meta_value(name: &str, value: &[u8]) -> Result<IndexDef> {
    if value.is_empty() {
        return Err(KayaError::corruption("index meta value empty"));
    }
    let version = value[0];
    match version {
        META_VERSION_V1 => {
            if value.len() < 6 {
                return Err(KayaError::corruption("index meta value too short"));
            }
            let unique = value[1] != 0;
            let prefix_len = u32::from_be_bytes(value[2..6].try_into().unwrap()) as usize;
            if value.len() != 6 + prefix_len {
                return Err(KayaError::corruption("index meta value length mismatch"));
            }
            Ok(IndexDef {
                name: name.to_string(),
                primary_prefix: value[6..].to_vec(),
                unique,
                extractor: IndexExtractor::WholeValue,
            })
        }
        META_VERSION_V2 => {
            if value.len() < 9 {
                return Err(KayaError::corruption("index meta v2 too short"));
            }
            let unique = value[1] != 0;
            let tag = value[2];
            let params_len = u16::from_be_bytes(value[3..5].try_into().unwrap()) as usize;
            if value.len() < 5 + params_len + 4 {
                return Err(KayaError::corruption("index meta v2 truncated"));
            }
            let params = &value[5..5 + params_len];
            let extractor = IndexExtractor::decode(tag, params)?;
            let rest = &value[5 + params_len..];
            let prefix_len = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
            if rest.len() != 4 + prefix_len {
                return Err(KayaError::corruption(
                    "index meta v2 prefix length mismatch",
                ));
            }
            Ok(IndexDef {
                name: name.to_string(),
                primary_prefix: rest[4..].to_vec(),
                unique,
                extractor,
            })
        }
        other => Err(KayaError::corruption(format!(
            "unsupported index meta version {other}"
        ))),
    }
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
    /// Create a secondary index over keys with `primary_prefix` (sync backfill, whole value).
    pub async fn create_index(&mut self, name: &str, primary_prefix: &[u8]) -> Result<()> {
        self.create_index_with(name, primary_prefix, CreateIndexOptions::default())
            .await
    }

    /// Create a secondary index with extractor and backfill mode.
    pub async fn create_index_with(
        &mut self,
        name: &str,
        primary_prefix: &[u8],
        opts: CreateIndexOptions,
    ) -> Result<()> {
        validate_index_name(name)?;
        if self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "index {name:?} already exists"
            )));
        }
        if primary_prefix.is_empty() {
            return Err(KayaError::invalid_argument(
                "primary_prefix must be non-empty",
            ));
        }
        self.validate_key(primary_prefix)?;

        let def = IndexDef {
            name: name.to_string(),
            primary_prefix: primary_prefix.to_vec(),
            unique: false,
            extractor: opts.extractor,
        };
        let meta_key = encode_meta_key(name);
        let meta_val = encode_meta_value(&def);
        let write_opts = WriteOptions::default();
        self.write_put(meta_key, meta_val, write_opts.clone())
            .await?;
        self.indexes.insert(name.to_string(), def);
        self.index_backfill.insert(
            name.to_string(),
            match opts.backfill {
                BackfillMode::Sync => BackfillProgress {
                    status: BackfillStatus::Idle,
                    ..Default::default()
                },
                BackfillMode::Online => BackfillProgress {
                    status: BackfillStatus::Running,
                    scanned: 0,
                    indexed: 0,
                    last_key: None,
                },
            },
        );

        match opts.backfill {
            BackfillMode::Sync => {
                self.run_sync_backfill(name, primary_prefix, &write_opts)
                    .await?;
            }
            BackfillMode::Online => {
                // Operator drives steps via `index_backfill_step`.
            }
        }
        Ok(())
    }

    async fn run_sync_backfill(
        &mut self,
        name: &str,
        primary_prefix: &[u8],
        opts: &WriteOptions,
    ) -> Result<()> {
        let existing = self.scan_prefix_inner(primary_prefix, ScanOptions::default())?;
        let mut scanned = 0u64;
        let mut indexed = 0u64;
        for kv in existing {
            if is_index_system_key(&kv.key) {
                continue;
            }
            scanned += 1;
            if self
                .insert_index_entries_for_named(name, &kv.key, &kv.value, opts)
                .await?
            {
                indexed += 1;
            }
        }
        self.index_backfill.insert(
            name.to_string(),
            BackfillProgress {
                status: BackfillStatus::Complete,
                scanned,
                indexed,
                last_key: None,
            },
        );
        Ok(())
    }

    /// Registered index names (sorted).
    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.keys().cloned().collect()
    }

    /// Return a clone of the index definition, if present.
    pub fn get_index(&self, name: &str) -> Option<IndexDef> {
        self.indexes.get(name).cloned()
    }

    /// Drop index metadata, data entries, and backfill state.
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
            if decode_data_key(name, &kv.key).is_some() {
                self.write_delete(kv.key, opts.clone()).await?;
            }
        }
        self.write_delete(encode_meta_key(name), opts).await?;
        self.indexes.remove(name);
        self.index_backfill.remove(name);
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
                if sec.starts_with(value_prefix) {
                    out.push((sec, pk));
                }
            }
        }
        Ok(out)
    }

    /// Current backfill progress for an index (defaults to Idle if never tracked).
    pub fn index_backfill_status(&self, name: &str) -> Result<BackfillProgress> {
        if !self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "unknown index {name:?}"
            )));
        }
        Ok(self.index_backfill.get(name).cloned().unwrap_or_default())
    }

    /// Pause an online backfill (no-op if already paused/complete/idle).
    pub fn index_backfill_pause(&mut self, name: &str) -> Result<BackfillProgress> {
        if !self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "unknown index {name:?}"
            )));
        }
        let prog = self.index_backfill.entry(name.to_string()).or_default();
        if prog.status == BackfillStatus::Running {
            prog.status = BackfillStatus::Paused;
        }
        Ok(prog.clone())
    }

    /// Resume a paused online backfill.
    pub fn index_backfill_resume(&mut self, name: &str) -> Result<BackfillProgress> {
        if !self.indexes.contains_key(name) {
            return Err(KayaError::invalid_argument(format!(
                "unknown index {name:?}"
            )));
        }
        let prog = self.index_backfill.entry(name.to_string()).or_default();
        match prog.status {
            BackfillStatus::Paused | BackfillStatus::Idle => {
                // Idle → treat as starting online backfill from scratch/cursor.
                if prog.status == BackfillStatus::Idle
                    && prog.last_key.is_none()
                    && prog.scanned == 0
                {
                    // Allow resume of a sync-created index as a re-scan (operator-driven).
                }
                prog.status = BackfillStatus::Running;
            }
            BackfillStatus::Complete => {
                return Err(KayaError::invalid_argument(format!(
                    "index {name:?} backfill already complete"
                )));
            }
            BackfillStatus::Running => {}
        }
        Ok(prog.clone())
    }

    /// Process up to `batch_size` primary keys for online backfill.
    ///
    /// Returns updated progress. Requires status `Running` (call resume after pause).
    pub async fn index_backfill_step(
        &mut self,
        name: &str,
        batch_size: usize,
    ) -> Result<BackfillProgress> {
        if batch_size == 0 {
            return self.index_backfill_status(name);
        }
        let def = self
            .indexes
            .get(name)
            .cloned()
            .ok_or_else(|| KayaError::invalid_argument(format!("unknown index {name:?}")))?;

        let mut prog = self.index_backfill.get(name).cloned().unwrap_or_default();
        if prog.status == BackfillStatus::Paused {
            return Err(KayaError::invalid_argument(format!(
                "index {name:?} backfill is paused; resume first"
            )));
        }
        if prog.status == BackfillStatus::Complete {
            return Ok(prog);
        }
        prog.status = BackfillStatus::Running;

        let prefix = def.primary_prefix.clone();
        let all = self.scan_prefix_inner(&prefix, ScanOptions::default())?;
        let start_after = prog.last_key.clone();
        let opts = WriteOptions::default();
        let mut processed = 0usize;

        for kv in all {
            if is_index_system_key(&kv.key) {
                continue;
            }
            if let Some(ref last) = start_after {
                if kv.key.as_slice() <= last.as_slice() {
                    continue;
                }
            }
            prog.scanned += 1;
            if self
                .insert_index_entries_for_named(name, &kv.key, &kv.value, &opts)
                .await?
            {
                prog.indexed += 1;
            }
            prog.last_key = Some(kv.key.clone());
            processed += 1;
            if processed >= batch_size {
                self.index_backfill.insert(name.to_string(), prog.clone());
                return Ok(prog);
            }
        }

        prog.status = BackfillStatus::Complete;
        self.index_backfill.insert(name.to_string(), prog.clone());
        Ok(prog)
    }

    /// Verify index entries against current primary values (divergence gate).
    ///
    /// Empty result means the index is consistent for the current latest snapshot.
    pub async fn verify_index(&mut self, name: &str) -> Result<Vec<IndexDivergence>> {
        let def = self
            .indexes
            .get(name)
            .cloned()
            .ok_or_else(|| KayaError::invalid_argument(format!("unknown index {name:?}")))?;

        let mut divergences = Vec::new();
        let mut expected: BTreeMap<Bytes, Bytes> = BTreeMap::new(); // primary -> secondary

        let primaries = self.scan_prefix_inner(&def.primary_prefix, ScanOptions::default())?;
        for kv in primaries {
            if is_index_system_key(&kv.key) {
                continue;
            }
            if let Some(sec) = def.extractor.extract(&kv.value) {
                expected.insert(kv.key.clone(), sec);
            }
        }

        let idx_entries =
            self.scan_prefix_inner(&encode_data_scan_prefix(name, b""), ScanOptions::default())?;
        let mut seen_primary: BTreeMap<Bytes, Bytes> = BTreeMap::new();
        for kv in idx_entries {
            let Some((sec, pk)) = decode_data_key(name, &kv.key) else {
                continue;
            };
            match expected.get(&pk) {
                Some(exp_sec) if exp_sec == &sec => {
                    seen_primary.insert(pk, sec);
                }
                Some(_) | None => {
                    divergences.push(IndexDivergence {
                        primary_key: pk.clone(),
                        kind: IndexDivergenceKind::ExtraInIndex { secondary: sec },
                    });
                }
            }
        }

        for (pk, sec) in &expected {
            if !seen_primary.contains_key(pk) {
                divergences.push(IndexDivergence {
                    primary_key: pk.clone(),
                    kind: IndexDivergenceKind::MissingInIndex {
                        expected_secondary: sec.clone(),
                    },
                });
            }
        }

        Ok(divergences)
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
        // Backfill state is ephemeral (not durable); mark Complete for recovered indexes.
        self.index_backfill = self
            .indexes
            .keys()
            .map(|n| {
                (
                    n.clone(),
                    BackfillProgress {
                        status: BackfillStatus::Complete,
                        ..Default::default()
                    },
                )
            })
            .collect();
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

        for def in &matching {
            if let Some(old) = old_value {
                if let Some(old_sec) = def.extractor.extract(old) {
                    let new_sec = def.extractor.extract(new_value);
                    if new_sec.as_ref() != Some(&old_sec) {
                        let old_ik = encode_data_key(&def.name, &old_sec, key);
                        self.write_delete(old_ik, opts.clone()).await?;
                    } else if let Some(ref sec) = new_sec {
                        // Unchanged secondary: ensure entry exists.
                        let ik = encode_data_key(&def.name, sec, key);
                        if self.get_inner(&ik, ReadTimestamp::Latest)?.is_none() {
                            self.write_put(ik, Bytes::new(), opts.clone()).await?;
                        }
                        continue;
                    }
                }
            }
            if let Some(sec) = def.extractor.extract(new_value) {
                let ik = encode_data_key(&def.name, &sec, key);
                self.write_put(ik, Bytes::new(), opts.clone()).await?;
            }
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
        let matching: Vec<IndexDef> = self
            .indexes
            .values()
            .filter(|d| key.starts_with(&d.primary_prefix))
            .cloned()
            .collect();
        for def in matching {
            if let Some(sec) = def.extractor.extract(old) {
                let ik = encode_data_key(&def.name, &sec, key);
                self.write_delete(ik, opts.clone()).await?;
            }
        }
        Ok(())
    }

    /// Insert index entry for one named index. Returns true if an entry was written.
    async fn insert_index_entries_for_named(
        &mut self,
        name: &str,
        key: &[u8],
        value: &[u8],
        opts: &WriteOptions,
    ) -> Result<bool> {
        let Some(def) = self.indexes.get(name).cloned() else {
            return Ok(false);
        };
        if !key.starts_with(&def.primary_prefix) {
            return Ok(false);
        }
        let Some(sec) = def.extractor.extract(value) else {
            return Ok(false);
        };
        let ik = encode_data_key(name, &sec, key);
        self.write_put(ik, Bytes::new(), opts.clone()).await?;
        Ok(true)
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
            .append(
                kaya_wal::WalPayload::Delete { key: key.clone() },
                durability,
            )
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
    fn meta_value_roundtrip_v2() {
        let def = IndexDef {
            name: "x".into(),
            primary_prefix: b"user:".to_vec(),
            unique: false,
            extractor: IndexExtractor::Field {
                delimiter: b'|',
                index: 1,
            },
        };
        let v = encode_meta_value(&def);
        let got = decode_meta_value("x", &v).unwrap();
        assert_eq!(got, def);
    }

    #[test]
    fn meta_value_v1_legacy_decodes() {
        // Hand-build v1: version=1, unique=0, prefix_len=5, "user:"
        let mut v = vec![1u8, 0];
        v.extend_from_slice(&5u32.to_be_bytes());
        v.extend_from_slice(b"user:");
        let got = decode_meta_value("legacy", &v).unwrap();
        assert_eq!(got.extractor, IndexExtractor::WholeValue);
        assert_eq!(got.primary_prefix, b"user:");
    }

    #[test]
    fn extractor_field_and_prefix() {
        let f = IndexExtractor::Field {
            delimiter: b',',
            index: 1,
        };
        assert_eq!(f.extract(b"a,b,c").as_deref(), Some(b"b".as_slice()));
        assert_eq!(f.extract(b"only").as_deref(), None);

        let p = IndexExtractor::Prefix { len: 3 };
        assert_eq!(p.extract(b"abcdef").as_deref(), Some(b"abc".as_slice()));
        assert_eq!(p.extract(b"ab").as_deref(), Some(b"ab".as_slice()));
    }
}
