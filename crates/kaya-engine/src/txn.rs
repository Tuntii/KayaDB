//! Single-node Snapshot Isolation transactions with in-memory write intents (M17 phase 1).
//!
//! Intents live only in process memory. On open/recovery the intent map is empty; the
//! distributed Raft commit path will re-establish durable intent/commit-record recovery.

use std::collections::{HashMap, HashSet};

use kaya_core::{Bytes, KayaError, Result, SequenceNumber};
use kaya_io::Disk;
use kaya_lsm::ValueRecordRef;

use super::{Engine, ReadTimestamp, WriteOptions};

/// Engine-local transaction identifier.
pub type TxnId = u64;

/// Provisional write held by a live transaction. `value = None` means delete intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub txn_id: TxnId,
    pub value: Option<Bytes>,
}

/// Per-transaction metadata for SI snapshot + intent cleanup.
#[derive(Debug, Clone)]
struct TxnMeta {
    /// Snapshot bound: reads use `ReadTimestamp::At(snapshot_ts)`.
    snapshot_ts: u64,
    /// Keys this txn currently holds intents on.
    keys: HashSet<Bytes>,
}

/// Intent tables owned by [`Engine`].
#[derive(Debug, Default)]
pub(crate) struct TxnTables {
    /// user_key → intent (at most one holder per key).
    intents: HashMap<Bytes, Intent>,
    /// Open transactions.
    txns: HashMap<TxnId, TxnMeta>,
    next_txn_id: TxnId,
}

impl TxnTables {
    pub(crate) fn new() -> Self {
        Self {
            intents: HashMap::new(),
            txns: HashMap::new(),
            // Start at 1 so 0 is never a valid live id (easier debugging).
            next_txn_id: 1,
        }
    }
}

impl<D: Disk> Engine<D> {
    /// Begin a transaction. Assigns `snapshot_ts = last_sequence` at BEGIN.
    pub fn begin_txn(&mut self) -> TxnId {
        let id = self.txn.next_txn_id;
        self.txn.next_txn_id = self.txn.next_txn_id.saturating_add(1);
        let snapshot_ts = self.stats.last_sequence;
        self.txn.txns.insert(
            id,
            TxnMeta {
                snapshot_ts,
                keys: HashSet::new(),
            },
        );
        id
    }

    /// Point read under the txn snapshot, with read-your-writes on own intents.
    pub fn txn_get(&mut self, txn_id: TxnId, key: &[u8]) -> Result<Option<Bytes>> {
        let meta = self.txn.txns.get(&txn_id).ok_or_else(|| {
            KayaError::invalid_argument(format!("unknown or finished transaction {txn_id}"))
        })?;
        let snapshot_ts = meta.snapshot_ts;

        // RYW: own intent first.
        if let Some(intent) = self.txn.intents.get(key) {
            if intent.txn_id == txn_id {
                return Ok(intent.value.clone());
            }
        }

        self.get_inner(key, ReadTimestamp::At(snapshot_ts))
    }

    /// Stage a put intent after write–write conflict checks.
    pub fn txn_put(&mut self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        self.validate_key(&key)?;
        self.validate_value(&value)?;
        self.stage_intent(txn_id, key, Some(value))
    }

    /// Stage a delete intent after write–write conflict checks.
    pub fn txn_delete(&mut self, txn_id: TxnId, key: Bytes) -> Result<()> {
        self.validate_key(&key)?;
        self.stage_intent(txn_id, key, None)
    }

    /// Materialize all intents via durable put/delete, then clear txn state.
    ///
    /// Each intent gets its own WAL sequence (sequential apply). `commit_ts` is
    /// the last sequence assigned, or current `last_sequence` if the txn wrote nothing.
    pub async fn txn_commit(&mut self, txn_id: TxnId) -> Result<SequenceNumber> {
        let meta = self.txn.txns.get(&txn_id).ok_or_else(|| {
            KayaError::invalid_argument(format!("unknown or finished transaction {txn_id}"))
        })?;
        let keys: Vec<Bytes> = meta.keys.iter().cloned().collect();
        let snapshot_ts = meta.snapshot_ts;

        // Re-check conflicts at commit (defensive; puts already checked).
        for key in &keys {
            self.check_write_conflict(txn_id, key, snapshot_ts)?;
        }

        let opts = WriteOptions::default();
        let mut last_seq = SequenceNumber::new(self.stats.last_sequence);

        // Stable order for deterministic materialization.
        let mut keys = keys;
        keys.sort();

        for key in keys {
            let intent = match self.txn.intents.get(&key) {
                Some(i) if i.txn_id == txn_id => i.clone(),
                _ => continue,
            };
            let wr = match intent.value {
                Some(value) => self.put(key.clone(), value, opts.clone()).await?,
                None => self.delete(key.clone(), opts.clone()).await?,
            };
            last_seq = wr.sequence;
            self.txn.intents.remove(&key);
        }

        self.txn.txns.remove(&txn_id);
        Ok(last_seq)
    }

    /// Discard all intents for `txn_id`.
    pub fn txn_rollback(&mut self, txn_id: TxnId) -> Result<()> {
        let meta = self.txn.txns.remove(&txn_id).ok_or_else(|| {
            KayaError::invalid_argument(format!("unknown or finished transaction {txn_id}"))
        })?;
        for key in meta.keys {
            if let Some(intent) = self.txn.intents.get(&key) {
                if intent.txn_id == txn_id {
                    self.txn.intents.remove(&key);
                }
            }
        }
        Ok(())
    }

    fn stage_intent(&mut self, txn_id: TxnId, key: Bytes, value: Option<Bytes>) -> Result<()> {
        let snapshot_ts = {
            let meta = self.txn.txns.get(&txn_id).ok_or_else(|| {
                KayaError::invalid_argument(format!("unknown or finished transaction {txn_id}"))
            })?;
            meta.snapshot_ts
        };

        self.check_write_conflict(txn_id, &key, snapshot_ts)?;

        self.txn.intents.insert(
            key.clone(),
            Intent {
                txn_id,
                value,
            },
        );
        if let Some(meta) = self.txn.txns.get_mut(&txn_id) {
            meta.keys.insert(key);
        }
        Ok(())
    }

    /// Intent conflict (other txn) or SI committed-version conflict (`seq > snapshot_ts`).
    fn check_write_conflict(
        &mut self,
        txn_id: TxnId,
        key: &[u8],
        snapshot_ts: u64,
    ) -> Result<()> {
        if let Some(intent) = self.txn.intents.get(key) {
            if intent.txn_id != txn_id {
                return Err(KayaError::TxnConflict);
            }
            // Own intent: overwrite allowed; still check committed SI below.
        }

        if let Some(seq) = self.latest_committed_seq(key)? {
            if seq > snapshot_ts {
                return Err(KayaError::TxnConflict);
            }
        }
        Ok(())
    }

    /// Highest committed sequence for `key`, if any version exists (put or delete).
    fn latest_committed_seq(&mut self, key: &[u8]) -> Result<Option<u64>> {
        if let Some(rec) = self.memtable.get(key) {
            let seq = match rec {
                ValueRecordRef::Put { sequence, .. } | ValueRecordRef::Delete { sequence } => {
                    sequence.get()
                }
            };
            return Ok(Some(seq));
        }
        for (_, reader) in &self.live_sstables {
            if let Some(entry) = reader.get(key)? {
                return Ok(Some(entry.sequence.get()));
            }
        }
        Ok(None)
    }
}
