//! Cross-shard 2PC durable records (M23).
//!
//! Participant groups persist prepare intents and a per-txn record under the
//! reserved system prefix `\x00txn/`. Commit materializes intents to user keys
//! via [`Engine::apply_mutations`]; abort deletes intents only.
//!
//! # System keys
//!
//! ```text
//! \x00txn/rec/{txn_id_be8}                 → Txn2pcState (1 byte)
//! \x00txn/intent/{txn_id_be8}/{user_key}   → intent payload
//! ```
//!
//! Intent payload: `0` = delete tombstone; `1 || value` = put.

use kaya_core::{Bytes, KayaError, Result};
use kaya_io::Disk;

use super::{Engine, ReadTimestamp, ScanOptions, WriteOptions};

/// Reserved system-key prefix for all 2PC records and durable intents.
pub const TXN_SYS_PREFIX: &[u8] = b"\x00txn/";
/// Transaction record keys: `\x00txn/rec/{txn_id_be8}`.
pub const TXN_REC_PREFIX: &[u8] = b"\x00txn/rec/";
/// Durable intent keys: `\x00txn/intent/{txn_id_be8}/{user_key}`.
pub const TXN_INTENT_PREFIX: &[u8] = b"\x00txn/intent/";

const INTENT_DELETE: u8 = 0;
const INTENT_PUT: u8 = 1;

/// Durable 2PC participant state stored under the rec key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Txn2pcState {
    Preparing = 1,
    Prepared = 2,
    Committed = 3,
    Aborted = 4,
    /// Durable commit decision accepted; materialization may be incomplete.
    /// Crash recovery must finish commit, never abort.
    Committing = 5,
}

impl Txn2pcState {
    pub fn as_byte(self) -> u8 {
        self as u8
    }

    pub fn from_byte(b: u8) -> Result<Self> {
        match b {
            1 => Ok(Self::Preparing),
            2 => Ok(Self::Prepared),
            3 => Ok(Self::Committed),
            4 => Ok(Self::Aborted),
            5 => Ok(Self::Committing),
            other => Err(KayaError::corruption(format!(
                "unknown 2PC txn state byte {other}"
            ))),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Preparing => "Preparing",
            Self::Prepared => "Prepared",
            Self::Committed => "Committed",
            Self::Aborted => "Aborted",
            Self::Committing => "Committing",
        }
    }
}

/// True if `key` is in the reserved 2PC system space.
pub fn is_txn_system_key(key: &[u8]) -> bool {
    key.starts_with(TXN_SYS_PREFIX)
}

/// `\x00txn/rec/{txn_id as u64 BE}`.
pub fn encode_rec_key(txn_id: u64) -> Bytes {
    let mut k = Vec::with_capacity(TXN_REC_PREFIX.len() + 8);
    k.extend_from_slice(TXN_REC_PREFIX);
    k.extend_from_slice(&txn_id.to_be_bytes());
    k
}

/// `\x00txn/intent/{txn_id as u64 BE}/{user_key}`.
pub fn encode_intent_key(txn_id: u64, user_key: &[u8]) -> Bytes {
    let mut k = Vec::with_capacity(TXN_INTENT_PREFIX.len() + 8 + user_key.len());
    k.extend_from_slice(TXN_INTENT_PREFIX);
    k.extend_from_slice(&txn_id.to_be_bytes());
    k.push(b'/');
    k.extend_from_slice(user_key);
    k
}

/// Prefix for scanning all intents belonging to `txn_id`.
pub fn encode_intent_scan_prefix(txn_id: u64) -> Bytes {
    let mut k = Vec::with_capacity(TXN_INTENT_PREFIX.len() + 8 + 1);
    k.extend_from_slice(TXN_INTENT_PREFIX);
    k.extend_from_slice(&txn_id.to_be_bytes());
    k.push(b'/');
    k
}

/// Decode user_key from a full intent system key for `txn_id`.
pub fn user_key_from_intent_key(txn_id: u64, intent_key: &[u8]) -> Option<&[u8]> {
    let prefix = encode_intent_scan_prefix(txn_id);
    intent_key.strip_prefix(prefix.as_slice())
}

/// Parse `txn_id` from a `\x00txn/rec/{txn_id_be8}` key.
pub fn parse_rec_txn_id(key: &[u8]) -> Option<u64> {
    if key.len() != TXN_REC_PREFIX.len() + 8 {
        return None;
    }
    if !key.starts_with(TXN_REC_PREFIX) {
        return None;
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&key[TXN_REC_PREFIX.len()..]);
    Some(u64::from_be_bytes(buf))
}

/// Counts returned by [`Engine::recover_incomplete_2pc`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Txn2pcRecoveryStats {
    /// Records that were `Preparing` / `Prepared` and aborted fail-closed.
    pub aborted: u32,
    /// Records that were `Committing` and finished to `Committed`.
    pub finished_commits: u32,
}

/// Encode an intent value: put or delete tombstone.
pub fn encode_intent_value(value: Option<&[u8]>) -> Bytes {
    match value {
        Some(v) => {
            let mut out = Vec::with_capacity(1 + v.len());
            out.push(INTENT_PUT);
            out.extend_from_slice(v);
            out
        }
        None => vec![INTENT_DELETE],
    }
}

/// Decode an intent value written by [`encode_intent_value`].
pub fn decode_intent_value(raw: &[u8]) -> Result<Option<Bytes>> {
    if raw.is_empty() {
        return Err(KayaError::corruption("empty 2PC intent value"));
    }
    match raw[0] {
        INTENT_DELETE => {
            if raw.len() != 1 {
                return Err(KayaError::corruption(
                    "2PC delete intent must be a single-byte tombstone",
                ));
            }
            Ok(None)
        }
        INTENT_PUT => Ok(Some(raw[1..].to_vec())),
        other => Err(KayaError::corruption(format!(
            "unknown 2PC intent tag {other}"
        ))),
    }
}

impl<D: Disk> Engine<D> {
    /// Persist durable prepare intents + mark the txn record `Prepared`.
    ///
    /// Writes `Preparing`, then each intent under `\x00txn/intent/…`, then
    /// flips the record to `Prepared`. Uses raw WAL/memtable writes so system
    /// keys are not rejected by the public API.
    pub async fn apply_txn_prepare(
        &mut self,
        txn_id: u64,
        mutations: &[(Bytes, Option<Bytes>)],
    ) -> Result<()> {
        let opts = WriteOptions::default();
        let rec_key = encode_rec_key(txn_id);
        self.write_put(
            rec_key.clone(),
            vec![Txn2pcState::Preparing.as_byte()],
            opts.clone(),
        )
        .await?;

        for (user_key, value) in mutations {
            if user_key.is_empty() {
                return Err(KayaError::invalid_argument(
                    "2PC prepare mutation key must not be empty",
                ));
            }
            if is_txn_system_key(user_key) || crate::index::is_index_system_key(user_key) {
                return Err(KayaError::invalid_argument(
                    "2PC prepare mutations must not target system keys",
                ));
            }
            let ik = encode_intent_key(txn_id, user_key);
            let iv = encode_intent_value(value.as_deref());
            self.write_put(ik, iv, opts.clone()).await?;
        }

        self.write_put(rec_key, vec![Txn2pcState::Prepared.as_byte()], opts)
            .await?;
        Ok(())
    }

    /// Materialize durable intents for `txn_id` then clear them and mark
    /// `Committed`.
    ///
    /// Crash-safe order:
    /// 1. Durably write `Committing` (commit decision) before any user-key write
    /// 2. `apply_mutations` + clear intents
    /// 3. Write `Committed`
    ///
    /// Recovery of `Committing` resumes this path (intents may already be partly
    /// applied or cleared). User-key materialization goes through
    /// [`Self::apply_mutations`] so index maintenance and CDC fire.
    pub async fn apply_txn_commit_2pc(&mut self, txn_id: u64) -> Result<()> {
        let opts = WriteOptions::default();
        let rec_key = encode_rec_key(txn_id);

        if let Some(state) = self.read_txn2pc_state(txn_id)? {
            if state == Txn2pcState::Committed {
                return Ok(());
            }
            if state == Txn2pcState::Aborted {
                return Err(KayaError::invalid_argument(format!(
                    "cannot commit aborted 2PC txn {txn_id}"
                )));
            }
            // Preparing / Prepared / Committing: ensure Committing is durable
            // before materializing (idempotent if already Committing).
            if state != Txn2pcState::Committing {
                self.write_put(
                    rec_key.clone(),
                    vec![Txn2pcState::Committing.as_byte()],
                    opts.clone(),
                )
                .await?;
            }
        } else {
            // No rec key (rare): still mark Committing so recovery finishes.
            self.write_put(
                rec_key.clone(),
                vec![Txn2pcState::Committing.as_byte()],
                opts.clone(),
            )
            .await?;
        }

        let mutations = self.load_durable_intents(txn_id)?;
        if !mutations.is_empty() {
            self.apply_mutations(mutations, opts.clone()).await?;
        }
        self.clear_durable_intents(txn_id).await?;
        self.write_put(rec_key, vec![Txn2pcState::Committed.as_byte()], opts)
            .await?;
        Ok(())
    }

    /// Drop durable intents for `txn_id` and mark the record `Aborted`.
    ///
    /// Does not touch user keys. Rejects `Committing` / `Committed` (decision
    /// already made).
    pub async fn apply_txn_abort_2pc(&mut self, txn_id: u64) -> Result<()> {
        let opts = WriteOptions::default();
        let rec_key = encode_rec_key(txn_id);

        if let Some(state) = self.read_txn2pc_state(txn_id)? {
            if state == Txn2pcState::Aborted {
                return Ok(());
            }
            if state == Txn2pcState::Committed || state == Txn2pcState::Committing {
                return Err(KayaError::invalid_argument(format!(
                    "cannot abort {} 2PC txn {txn_id}",
                    state.name()
                )));
            }
        }

        self.clear_durable_intents(txn_id).await?;
        self.write_put(rec_key, vec![Txn2pcState::Aborted.as_byte()], opts)
            .await?;
        Ok(())
    }

    /// Read the durable 2PC record state, if present.
    pub fn read_txn2pc_state(&mut self, txn_id: u64) -> Result<Option<Txn2pcState>> {
        let rec_key = encode_rec_key(txn_id);
        match self.get_inner(&rec_key, ReadTimestamp::Latest)? {
            Some(raw) if !raw.is_empty() => Ok(Some(Txn2pcState::from_byte(raw[0])?)),
            Some(_) => Err(KayaError::corruption("empty 2PC rec value")),
            None => Ok(None),
        }
    }

    /// Crash recovery for incomplete 2PC participant records (run on open).
    ///
    /// - `Preparing` / `Prepared` → abort (fail-closed; no durable commit decision)
    /// - `Committing` → finish commit (decision was durable; never abort)
    /// - `Committed` / `Aborted` → leave untouched
    ///
    /// Safe to call multiple times (idempotent).
    pub async fn recover_incomplete_2pc(&mut self) -> Result<Txn2pcRecoveryStats> {
        // Use the internal scan so open-time recovery does not pollute user-facing
        // scan latency histograms (see read_path_latency_histograms_are_populated).
        let rows = self.scan_prefix_inner(TXN_REC_PREFIX, ScanOptions::default())?;

        let mut stats = Txn2pcRecoveryStats::default();
        for kv in rows {
            let Some(txn_id) = parse_rec_txn_id(&kv.key) else {
                continue;
            };
            if kv.value.is_empty() {
                continue;
            }
            let state = Txn2pcState::from_byte(kv.value[0])?;
            match state {
                Txn2pcState::Preparing | Txn2pcState::Prepared => {
                    self.apply_txn_abort_2pc(txn_id).await?;
                    stats.aborted = stats.aborted.saturating_add(1);
                }
                Txn2pcState::Committing => {
                    self.apply_txn_commit_2pc(txn_id).await?;
                    stats.finished_commits = stats.finished_commits.saturating_add(1);
                }
                Txn2pcState::Committed | Txn2pcState::Aborted => {}
            }
        }
        Ok(stats)
    }

    fn load_durable_intents(&mut self, txn_id: u64) -> Result<Vec<(Bytes, Option<Bytes>)>> {
        let prefix = encode_intent_scan_prefix(txn_id);
        let rows = self.scan_prefix_inner(&prefix, ScanOptions::default())?;
        let mut out = Vec::with_capacity(rows.len());
        for kv in rows {
            let user_key = user_key_from_intent_key(txn_id, &kv.key).ok_or_else(|| {
                KayaError::corruption(format!(
                    "intent key missing txn {txn_id} prefix: {:?}",
                    kv.key
                ))
            })?;
            let value = decode_intent_value(&kv.value)?;
            out.push((user_key.to_vec(), value));
        }
        Ok(out)
    }

    async fn clear_durable_intents(&mut self, txn_id: u64) -> Result<()> {
        let prefix = encode_intent_scan_prefix(txn_id);
        let rows = self.scan_prefix_inner(&prefix, ScanOptions::default())?;
        let opts = WriteOptions::default();
        for kv in rows {
            self.write_delete(kv.key, opts.clone()).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaya_core::{DurabilityMode, EngineConfig};
    use kaya_io::SimDisk;

    use super::*;
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
    fn rec_and_intent_key_layout() {
        let rec = encode_rec_key(0x1122_3344_5566_7788);
        assert!(rec.starts_with(TXN_REC_PREFIX));
        assert_eq!(
            &rec[TXN_REC_PREFIX.len()..],
            &0x1122_3344_5566_7788u64.to_be_bytes()
        );

        let ik = encode_intent_key(42, b"user/a");
        assert!(ik.starts_with(TXN_INTENT_PREFIX));
        let prefix = encode_intent_scan_prefix(42);
        assert!(ik.starts_with(&prefix));
        assert_eq!(
            user_key_from_intent_key(42, &ik),
            Some(b"user/a".as_slice())
        );
    }

    #[test]
    fn intent_value_roundtrip() {
        assert_eq!(
            decode_intent_value(&encode_intent_value(None)).unwrap(),
            None
        );
        assert_eq!(
            decode_intent_value(&encode_intent_value(Some(b"hello"))).unwrap(),
            Some(b"hello".to_vec())
        );
        assert_eq!(
            decode_intent_value(&encode_intent_value(Some(b""))).unwrap(),
            Some(b"".to_vec())
        );
    }

    #[test]
    fn state_byte_roundtrip() {
        for s in [
            Txn2pcState::Preparing,
            Txn2pcState::Prepared,
            Txn2pcState::Committed,
            Txn2pcState::Aborted,
            Txn2pcState::Committing,
        ] {
            assert_eq!(Txn2pcState::from_byte(s.as_byte()).unwrap(), s);
        }
    }

    #[test]
    fn prepare_holds_durable_intents_under_sys_prefix() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let mutations = vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"b".to_vec(), None),
                (b"c".to_vec(), Some(b"three".to_vec())),
            ];
            engine.apply_txn_prepare(7, &mutations).await.unwrap();

            assert_eq!(
                engine.read_txn2pc_state(7).unwrap(),
                Some(Txn2pcState::Prepared)
            );

            // User keys not yet visible.
            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(
                engine.get(b"c", ReadOptions::default()).await.unwrap(),
                None
            );

            // Intents live under system prefix.
            let intent_a = encode_intent_key(7, b"a");
            let raw = engine
                .get(&intent_a, ReadOptions::default())
                .await
                .unwrap()
                .expect("intent a");
            assert_eq!(decode_intent_value(&raw).unwrap(), Some(b"1".to_vec()));

            let intent_b = encode_intent_key(7, b"b");
            let raw_b = engine
                .get(&intent_b, ReadOptions::default())
                .await
                .unwrap()
                .expect("intent b");
            assert_eq!(decode_intent_value(&raw_b).unwrap(), None);

            // Rec key present.
            let rec = engine
                .get(&encode_rec_key(7), ReadOptions::default())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(rec[0], Txn2pcState::Prepared.as_byte());
        });
    }

    #[test]
    fn prepare_commit_materializes_and_clears_intents() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            engine
                .put(b"del-me".to_vec(), b"gone".to_vec(), strict_opts())
                .await
                .unwrap();

            let mutations = vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"del-me".to_vec(), None),
            ];
            engine.apply_txn_prepare(9, &mutations).await.unwrap();
            engine.apply_txn_commit_2pc(9).await.unwrap();

            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine.get(b"del-me", ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(
                engine.read_txn2pc_state(9).unwrap(),
                Some(Txn2pcState::Committed)
            );
            // Intent keys cleared.
            assert_eq!(
                engine
                    .get(&encode_intent_key(9, b"a"), ReadOptions::default())
                    .await
                    .unwrap(),
                None
            );
            // Idempotent second commit.
            engine.apply_txn_commit_2pc(9).await.unwrap();
        });
    }

    #[test]
    fn prepare_abort_clears_intents_only() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            engine
                .put(b"keep".to_vec(), b"yes".to_vec(), strict_opts())
                .await
                .unwrap();

            let mutations = vec![
                (b"a".to_vec(), Some(b"1".to_vec())),
                (b"keep".to_vec(), None),
            ];
            engine.apply_txn_prepare(3, &mutations).await.unwrap();
            engine.apply_txn_abort_2pc(3).await.unwrap();

            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(
                engine.get(b"keep", ReadOptions::default()).await.unwrap(),
                Some(b"yes".to_vec())
            );
            assert_eq!(
                engine.read_txn2pc_state(3).unwrap(),
                Some(Txn2pcState::Aborted)
            );
            assert_eq!(
                engine
                    .get(&encode_intent_key(3, b"a"), ReadOptions::default())
                    .await
                    .unwrap(),
                None
            );
            // Idempotent second abort.
            engine.apply_txn_abort_2pc(3).await.unwrap();
        });
    }

    #[test]
    fn prepare_without_commit_aborts_on_reopen() {
        // Incomplete 2PC (Prepared, no durable commit decision) is fail-closed
        // on Engine::open — intents and partial prepares must not leak.
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            {
                let mut engine = Engine::open(EngineConfig::default(), disk.clone())
                    .await
                    .unwrap();
                engine
                    .apply_txn_prepare(11, &[(b"x".to_vec(), Some(b"durable".to_vec()))])
                    .await
                    .unwrap();
                assert_eq!(
                    engine.read_txn2pc_state(11).unwrap(),
                    Some(Txn2pcState::Prepared)
                );
            }
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            assert!(
                engine.last_recovery().txn2pc_aborted >= 1,
                "Prepared must abort on open"
            );
            assert_eq!(
                engine.read_txn2pc_state(11).unwrap(),
                Some(Txn2pcState::Aborted)
            );
            assert_eq!(
                engine
                    .get(&encode_intent_key(11, b"x"), ReadOptions::default())
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                engine.get(b"x", ReadOptions::default()).await.unwrap(),
                None
            );
        });
    }

    /// Crash mid-commit: durable `Committing` + remaining intents must finish
    /// to user-visible keys and `Committed` (never abort).
    #[test]
    fn committing_with_intents_finishes_on_recover() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            let mutations = vec![
                (b"user/a".to_vec(), Some(b"alpha".to_vec())),
                (b"user/b".to_vec(), Some(b"beta".to_vec())),
            ];
            engine.apply_txn_prepare(42, &mutations).await.unwrap();
            assert_eq!(
                engine.read_txn2pc_state(42).unwrap(),
                Some(Txn2pcState::Prepared)
            );

            // Simulate crash after decision, before materialization.
            engine
                .write_put(
                    encode_rec_key(42),
                    vec![Txn2pcState::Committing.as_byte()],
                    WriteOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(
                engine.read_txn2pc_state(42).unwrap(),
                Some(Txn2pcState::Committing)
            );
            // Intents still present; user keys not yet visible.
            assert!(engine
                .get(&encode_intent_key(42, b"user/a"), ReadOptions::default())
                .await
                .unwrap()
                .is_some());
            assert_eq!(
                engine.get(b"user/a", ReadOptions::default()).await.unwrap(),
                None
            );

            // Recovery / finish path.
            engine.apply_txn_commit_2pc(42).await.unwrap();

            assert_eq!(
                engine.get(b"user/a", ReadOptions::default()).await.unwrap(),
                Some(b"alpha".to_vec())
            );
            assert_eq!(
                engine.get(b"user/b", ReadOptions::default()).await.unwrap(),
                Some(b"beta".to_vec())
            );
            assert_eq!(
                engine.read_txn2pc_state(42).unwrap(),
                Some(Txn2pcState::Committed)
            );
            assert_eq!(
                engine
                    .get(&encode_intent_key(42, b"user/a"), ReadOptions::default())
                    .await
                    .unwrap(),
                None
            );
            // Abort must not undo a Committing decision that finished.
            assert!(engine.apply_txn_abort_2pc(42).await.is_err());
        });
    }

    #[test]
    fn abort_rejects_committing_state() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            engine
                .apply_txn_prepare(5, &[(b"k".to_vec(), Some(b"v".to_vec()))])
                .await
                .unwrap();
            engine
                .write_put(
                    encode_rec_key(5),
                    vec![Txn2pcState::Committing.as_byte()],
                    WriteOptions::default(),
                )
                .await
                .unwrap();
            let err = engine.apply_txn_abort_2pc(5).await.unwrap_err();
            assert!(
                err.to_string().contains("cannot abort"),
                "unexpected err: {err}"
            );
        });
    }

    #[test]
    fn open_aborts_preparing_and_prepared_clears_intents() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            {
                let mut engine = Engine::open(EngineConfig::default(), disk.clone())
                    .await
                    .unwrap();
                // Mid-prepare: Preparing + one intent (never flipped to Prepared).
                engine
                    .write_put(
                        encode_rec_key(100),
                        vec![Txn2pcState::Preparing.as_byte()],
                        WriteOptions::default(),
                    )
                    .await
                    .unwrap();
                engine
                    .write_put(
                        encode_intent_key(100, b"orphan"),
                        encode_intent_value(Some(b"x")),
                        WriteOptions::default(),
                    )
                    .await
                    .unwrap();
                engine
                    .apply_txn_prepare(101, &[(b"prep".to_vec(), Some(b"y".to_vec()))])
                    .await
                    .unwrap();
            }
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            assert!(engine.last_recovery().txn2pc_aborted >= 2);
            assert_eq!(
                engine.read_txn2pc_state(100).unwrap(),
                Some(Txn2pcState::Aborted)
            );
            assert_eq!(
                engine.read_txn2pc_state(101).unwrap(),
                Some(Txn2pcState::Aborted)
            );
            assert_eq!(
                engine
                    .get(&encode_intent_key(100, b"orphan"), ReadOptions::default())
                    .await
                    .unwrap(),
                None
            );
            assert_eq!(
                engine.get(b"prep", ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(
                engine.get(b"orphan", ReadOptions::default()).await.unwrap(),
                None
            );
        });
    }

    #[test]
    fn open_finishes_committing_across_reopen() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            {
                let mut engine = Engine::open(EngineConfig::default(), disk.clone())
                    .await
                    .unwrap();
                engine
                    .apply_txn_prepare(
                        200,
                        &[
                            (b"a".to_vec(), Some(b"1".to_vec())),
                            (b"b".to_vec(), Some(b"2".to_vec())),
                        ],
                    )
                    .await
                    .unwrap();
                engine
                    .write_put(
                        encode_rec_key(200),
                        vec![Txn2pcState::Committing.as_byte()],
                        WriteOptions::default(),
                    )
                    .await
                    .unwrap();
            }
            let mut engine = Engine::open(EngineConfig::default(), disk.clone())
                .await
                .unwrap();
            assert_eq!(engine.last_recovery().txn2pc_finished_commits, 1);
            assert_eq!(
                engine.read_txn2pc_state(200).unwrap(),
                Some(Txn2pcState::Committed)
            );
            assert_eq!(
                engine.get(b"a", ReadOptions::default()).await.unwrap(),
                Some(b"1".to_vec())
            );
            assert_eq!(
                engine.get(b"b", ReadOptions::default()).await.unwrap(),
                Some(b"2".to_vec())
            );
            // Idempotent second open.
            let engine2 = Engine::open(EngineConfig::default(), disk).await.unwrap();
            assert_eq!(engine2.last_recovery().txn2pc_finished_commits, 0);
        });
    }

    #[test]
    fn open_finishes_committing_after_partial_materialization() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            {
                let mut engine = Engine::open(EngineConfig::default(), disk.clone())
                    .await
                    .unwrap();
                engine
                    .apply_txn_prepare(
                        300,
                        &[
                            (b"u1".to_vec(), Some(b"one".to_vec())),
                            (b"u2".to_vec(), Some(b"two".to_vec())),
                        ],
                    )
                    .await
                    .unwrap();
                // Crash after decision + partial user-key write (u1 only).
                engine
                    .write_put(
                        encode_rec_key(300),
                        vec![Txn2pcState::Committing.as_byte()],
                        WriteOptions::default(),
                    )
                    .await
                    .unwrap();
                engine
                    .put(b"u1".to_vec(), b"one".to_vec(), strict_opts())
                    .await
                    .unwrap();
                // Intent for u1 still present (clear not done); u2 intent remains.
            }
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();
            assert_eq!(engine.last_recovery().txn2pc_finished_commits, 1);
            assert_eq!(
                engine.get(b"u1", ReadOptions::default()).await.unwrap(),
                Some(b"one".to_vec())
            );
            assert_eq!(
                engine.get(b"u2", ReadOptions::default()).await.unwrap(),
                Some(b"two".to_vec())
            );
            assert_eq!(
                engine.read_txn2pc_state(300).unwrap(),
                Some(Txn2pcState::Committed)
            );
            assert_eq!(
                engine
                    .get(&encode_intent_key(300, b"u1"), ReadOptions::default())
                    .await
                    .unwrap(),
                None
            );
        });
    }

    #[test]
    fn parse_rec_txn_id_roundtrip() {
        let k = encode_rec_key(0x0102_0304_0506_0708);
        assert_eq!(parse_rec_txn_id(&k), Some(0x0102_0304_0506_0708));
        assert_eq!(parse_rec_txn_id(b"nope"), None);
        assert_eq!(parse_rec_txn_id(TXN_REC_PREFIX), None);
    }
}
