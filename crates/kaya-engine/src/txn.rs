//! Single-node Snapshot Isolation transactions with in-memory write intents (M17 phase 1).
//!
//! Intents live only in process memory. On open/recovery the intent map is empty; the
//! distributed Raft commit path re-establishes durable multi-key atomicity via a single
//! [`RaftCommand::TxnCommit`](kaya_raft is not a dependency here — see kaya-server) entry
//! applied with [`Engine::apply_mutations`].

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
    ///
    /// Returns `(txn_id, snapshot_ts)`.
    pub fn begin_txn(&mut self) -> (TxnId, u64) {
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
        (id, snapshot_ts)
    }

    /// Snapshot timestamp assigned at BEGIN for an open transaction.
    pub fn txn_snapshot_ts(&self, txn_id: TxnId) -> Result<u64> {
        self.txn
            .txns
            .get(&txn_id)
            .map(|m| m.snapshot_ts)
            .ok_or_else(|| {
                KayaError::invalid_argument(format!("unknown or finished transaction {txn_id}"))
            })
    }

    /// Re-check write conflicts and return staged intents (sorted by key) without
    /// materializing or clearing them.
    ///
    /// Prefer [`Self::txn_take_commit`] for the production Raft path (clears local
    /// txn state so apply is pure and cannot double-apply intents).
    pub fn txn_prepare_commit(&mut self, txn_id: TxnId) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        let meta = self.txn.txns.get(&txn_id).ok_or_else(|| {
            KayaError::invalid_argument(format!("unknown or finished transaction {txn_id}"))
        })?;
        let mut keys: Vec<Bytes> = meta.keys.iter().cloned().collect();
        let snapshot_ts = meta.snapshot_ts;

        for key in &keys {
            self.check_write_conflict(txn_id, key, snapshot_ts)?;
        }

        keys.sort();
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            match self.txn.intents.get(&key) {
                Some(i) if i.txn_id == txn_id => out.push((key, i.value.clone())),
                _ => {}
            }
        }
        Ok(out)
    }

    /// Conflict-check, extract mutations, and **remove** the transaction + intents.
    ///
    /// Used by the Raft commit path: mutations are proposed as a single
    /// `TxnCommit` entry and applied on all nodes via [`Self::apply_mutations`].
    /// If the subsequent propose fails, the client must restart the transaction
    /// (intents are intentionally gone — production-acceptable fail-closed).
    pub fn txn_take_commit(&mut self, txn_id: TxnId) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        let mutations = self.txn_prepare_commit(txn_id)?;
        // Remove txn + intents after successful conflict check.
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
        Ok(mutations)
    }

    /// Drop txn metadata and intents after external materialization (Raft) or
    /// as a synonym for rollback cleanup.
    pub fn txn_finish(&mut self, txn_id: TxnId) -> Result<()> {
        self.txn_rollback(txn_id)
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
        crate::index::reject_if_system_key(&key)?;
        self.validate_value(&value)?;
        self.stage_intent(txn_id, key, Some(value))
    }

    /// Stage a delete intent after write–write conflict checks.
    pub fn txn_delete(&mut self, txn_id: TxnId, key: Bytes) -> Result<()> {
        self.validate_key(&key)?;
        crate::index::reject_if_system_key(&key)?;
        self.stage_intent(txn_id, key, None)
    }

    /// Apply all mutations durably as one logical commit.
    ///
    /// Each mutation still gets its own WAL sequence via existing `put`/`delete`
    /// paths (so index maintenance and CDC fire). Mutations are applied without
    /// yielding to other engine ops mid-batch (caller holds `&mut self`).
    ///
    /// **Durability note:** true single-fsync multi-record WAL atomicity is not
    /// provided here — each put/delete is individually WAL-protected. Production
    /// multi-key all-or-nothing is guaranteed by **Raft atomicity**: a single
    /// `TxnCommit` log entry is applied completely or not at all on recovery.
    ///
    /// Returns the last [`SequenceNumber`] assigned (commit_ts), or the current
    /// `last_sequence` when `mutations` is empty.
    pub async fn apply_mutations(
        &mut self,
        mutations: Vec<(Bytes, Option<Bytes>)>,
        opts: WriteOptions,
    ) -> Result<SequenceNumber> {
        let mut last_seq = SequenceNumber::new(self.stats.last_sequence);
        for (key, value) in mutations {
            let wr = match value {
                Some(value) => self.put(key, value, opts.clone()).await?,
                None => self.delete(key, opts.clone()).await?,
            };
            last_seq = wr.sequence;
        }
        Ok(last_seq)
    }

    /// Materialize prepared intents as durable mutations then finish (single-node).
    ///
    /// Uses [`Self::txn_take_commit`] → [`Self::apply_mutations`] so local commit
    /// shares the same materialization path as the Raft apply path.
    ///
    /// Each intent gets its own WAL sequence (sequential apply). `commit_ts` is
    /// the last sequence assigned, or current `last_sequence` if the txn wrote nothing.
    pub async fn txn_commit(&mut self, txn_id: TxnId) -> Result<SequenceNumber> {
        let mutations = self.txn_take_commit(txn_id)?;
        self.apply_mutations(mutations, WriteOptions::default())
            .await
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

        self.txn
            .intents
            .insert(key.clone(), Intent { txn_id, value });
        if let Some(meta) = self.txn.txns.get_mut(&txn_id) {
            meta.keys.insert(key);
        }
        Ok(())
    }

    /// Intent conflict (other txn) or SI committed-version conflict (`seq > snapshot_ts`).
    fn check_write_conflict(&mut self, txn_id: TxnId, key: &[u8], snapshot_ts: u64) -> Result<()> {
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

#[cfg(test)]
mod apply_mutations_tests {
    use std::sync::Arc;

    use kaya_core::{DurabilityMode, EngineConfig, KayaError};
    use kaya_io::SimDisk;

    use crate::{Engine, ReadOptions, WriteOptions};

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn strict_opts() -> WriteOptions {
        WriteOptions {
            durability: Some(DurabilityMode::Strict),
            ..WriteOptions::default()
        }
    }

    #[test]
    fn apply_mutations_put_and_delete() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            engine
                .put(b"del-me".to_vec(), b"gone".to_vec(), strict_opts())
                .await
                .unwrap();

            let commit_ts = engine
                .apply_mutations(
                    vec![
                        (b"a".to_vec(), Some(b"1".to_vec())),
                        (b"b".to_vec(), Some(b"2".to_vec())),
                        (b"del-me".to_vec(), None),
                    ],
                    strict_opts(),
                )
                .await
                .unwrap();
            assert!(commit_ts.get() > 0);
            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine.get(b"b", ReadOptions::default()).await.unwrap(),
                Some(b"2".to_vec())
            );
            assert_eq!(
                engine.get(b"del-me", ReadOptions::default()).await.unwrap(),
                None
            );
        });
    }

    #[test]
    fn apply_mutations_empty_returns_current_sequence() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            engine
                .put(b"k".to_vec(), b"v".to_vec(), strict_opts())
                .await
                .unwrap();
            let before = engine.stats().last_sequence;
            let seq = engine
                .apply_mutations(vec![], WriteOptions::default())
                .await
                .unwrap();
            assert_eq!(seq.get(), before);
        });
    }

    #[test]
    fn txn_take_commit_clears_state() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            let (t, _) = engine.begin_txn();
            engine.txn_put(t, b"a".to_vec(), b"1".to_vec()).unwrap();
            engine.txn_put(t, b"b".to_vec(), b"2".to_vec()).unwrap();
            engine.txn_delete(t, b"c".to_vec()).unwrap();

            let mutations = engine.txn_take_commit(t).unwrap();
            assert_eq!(mutations.len(), 3);
            assert_eq!(mutations[0].0, b"a");
            assert_eq!(mutations[0].1, Some(b"1".to_vec()));
            assert_eq!(mutations[1].0, b"b");
            assert_eq!(mutations[2].0, b"c");
            assert_eq!(mutations[2].1, None);

            // Txn gone; intents not visible / not held.
            assert!(matches!(
                engine.txn_get(t, b"a"),
                Err(KayaError::InvalidArgument { .. })
            ));
            assert!(matches!(
                engine.txn_take_commit(t),
                Err(KayaError::InvalidArgument { .. })
            ));
            // Keys not yet materialised.
            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                None
            );

            // Another txn can now stage the same keys.
            let (t2, _) = engine.begin_txn();
            engine.txn_put(t2, b"a".to_vec(), b"x".to_vec()).unwrap();
            engine.txn_rollback(t2).unwrap();
        });
    }

    #[test]
    fn txn_commit_uses_take_then_apply() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            let (t, _) = engine.begin_txn();
            engine.txn_put(t, b"x".to_vec(), b"y".to_vec()).unwrap();
            engine.txn_put(t, b"p".to_vec(), b"q".to_vec()).unwrap();
            let commit_ts = engine.txn_commit(t).await.unwrap();
            assert!(commit_ts.get() > 0);
            assert_eq!(
                engine.get(b"x", ReadOptions::default()).await.unwrap(),
                Some(b"y".to_vec())
            );
            assert_eq!(
                engine.get(b"p", ReadOptions::default()).await.unwrap(),
                Some(b"q".to_vec())
            );
            assert!(matches!(
                engine.txn_finish(t),
                Err(KayaError::InvalidArgument { .. })
            ));
        });
    }

    /// Raft apply path uses put/delete, so index + CDC must fire for batch commits.
    #[test]
    fn apply_mutations_maintains_index_and_cdc() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut cfg = EngineConfig::default();
            cfg.enable_cdc = true;
            let mut engine = Engine::open(cfg, disk).await.unwrap();
            engine.create_index("by_val", b"user:").await.unwrap();

            engine
                .apply_mutations(
                    vec![
                        (b"user:1".to_vec(), Some(b"alice".to_vec())),
                        (b"user:2".to_vec(), Some(b"bob".to_vec())),
                    ],
                    strict_opts(),
                )
                .await
                .unwrap();

            let hits = engine.scan_by_index("by_val", b"alice").await.unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].1, b"user:1");

            let mut cursor = engine.cdc_subscribe("raft-apply", None).unwrap();
            let events = engine.cdc_poll(&mut cursor, 10).unwrap();
            assert!(
                events.len() >= 2,
                "expected CDC events for batch mutations, got {}",
                events.len()
            );
            assert!(events.iter().any(|e| e.key == b"user:1"));
            assert!(events.iter().any(|e| e.key == b"user:2"));
        });
    }
}
