use std::collections::BTreeMap;

use kaya_raft::RaftCommand;

/// Multi-version in-memory reference model for sim invariants.
///
/// Stores every put/delete as a version keyed by commit timestamp (sequence):
/// `user_key → BTreeMap<commit_ts, Option<value>>` where `None` is a tombstone.
///
/// Latest reads (`get` / `scan_prefix`) match LWW-visible semantics. Snapshot
/// reads (`get_at` / `scan_prefix_at`) apply MVCC visibility:
/// newest version with `commit_ts ≤ read_ts`; Put → value, Delete/missing → None.
pub struct RefModel {
    /// user_key → (commit_ts → Some(value) | None tombstone)
    versions: BTreeMap<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>>,
    /// Next synthetic sequence for [`Self::apply_log_entry`] when no seq is known.
    next_seq: u64,
}

impl RefModel {
    pub fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            next_seq: 1,
        }
    }

    /// Record a put at the given commit sequence.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, seq: u64) {
        self.versions
            .entry(key)
            .or_default()
            .insert(seq, Some(value));
        self.bump_next_seq(seq);
    }

    /// Record a delete (tombstone) at the given commit sequence.
    pub fn delete(&mut self, key: &[u8], seq: u64) {
        self.versions
            .entry(key.to_vec())
            .or_default()
            .insert(seq, None);
        self.bump_next_seq(seq);
    }

    /// Latest-visible value (LWW): newest non-tombstone put, or `None`.
    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.get_at(key, u64::MAX)
    }

    /// Snapshot read: newest version with `commit_ts ≤ read_ts`.
    /// Put → value; Delete or no visible version → `None`.
    pub fn get_at(&self, key: &[u8], read_ts: u64) -> Option<&Vec<u8>> {
        let versions = self.versions.get(key)?;
        // BTreeMap is ordered by commit_ts ASC; walk reverse for newest ≤ read_ts.
        for (&ts, val) in versions.iter().rev() {
            if ts <= read_ts {
                return val.as_ref();
            }
        }
        None
    }

    /// All commit timestamps recorded for `key` (ascending).
    pub fn versions_of(&self, key: &[u8]) -> Vec<u64> {
        self.versions
            .get(key)
            .map(|m| m.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Every commit timestamp present in the model (ascending, unique).
    pub fn all_commit_timestamps(&self) -> Vec<u64> {
        let mut set = BTreeMap::<u64, ()>::new();
        for versions in self.versions.values() {
            for &ts in versions.keys() {
                set.insert(ts, ());
            }
        }
        set.into_keys().collect()
    }

    /// Apply a replicated Raft log entry payload to this state machine.
    /// Empty payloads are leader no-ops and are ignored.
    ///
    /// Raft commands do not carry a sequence; a synthetic increasing seq is
    /// assigned so multi-version history is still maintained for the model.
    pub fn apply_log_entry(&mut self, command: &[u8]) -> Result<(), String> {
        if command.is_empty() {
            return Ok(());
        }
        match RaftCommand::decode(command)? {
            RaftCommand::Put { key, value } => {
                let seq = self.alloc_seq();
                self.put(key, value, seq);
                Ok(())
            }
            RaftCommand::Delete { key } => {
                let seq = self.alloc_seq();
                self.delete(&key, seq);
                Ok(())
            }
            RaftCommand::ConfigChange { .. } => Ok(()),
            RaftCommand::TxnCommit { mutations, .. } => {
                for (key, value) in mutations {
                    let seq = self.alloc_seq();
                    match value {
                        Some(v) => self.put(key, v, seq),
                        None => self.delete(&key, seq),
                    }
                }
                Ok(())
            }
            // 2PC prepare/abort only touch system keys; the ref model tracks
            // user keys. Commit materializes like TxnCommit once coordinator
            // work is fully modelled (M23 task 10+).
            RaftCommand::TxnPrepare { .. }
            | RaftCommand::TxnCommit2pc { .. }
            | RaftCommand::TxnAbort2pc { .. }
            | RaftCommand::TxnDecision { .. }
            | RaftCommand::RangeMeta { .. } => Ok(()),
        }
    }

    /// Return all live `(key, value)` pairs whose key starts with `prefix`,
    /// using Latest visibility, in sorted key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.scan_prefix_at(prefix, u64::MAX)
    }

    /// Snapshot scan: for each user_key under `prefix`, emit the visible put
    /// at `read_ts` (if any), in sorted key order.
    pub fn scan_prefix_at(&self, prefix: &[u8], read_ts: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.versions
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter_map(|(k, _)| self.get_at(k, read_ts).map(|v| (k.clone(), v.clone())))
            .collect()
    }

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        s
    }

    fn bump_next_seq(&mut self, seq: u64) {
        let next = seq.saturating_add(1);
        if next > self.next_seq {
            self.next_seq = next;
        }
    }
}

impl Default for RefModel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multi_version_put_get_at() {
        let mut m = RefModel::new();
        m.put(b"k".to_vec(), b"v1".to_vec(), 1);
        m.put(b"k".to_vec(), b"v2".to_vec(), 2);

        assert_eq!(
            m.get_at(b"k", 1).map(|v| v.as_slice()),
            Some(b"v1".as_ref())
        );
        assert_eq!(
            m.get_at(b"k", 2).map(|v| v.as_slice()),
            Some(b"v2".as_ref())
        );
        assert_eq!(m.get(b"k").map(|v| v.as_slice()), Some(b"v2".as_ref()));
        assert_eq!(m.get_at(b"k", 0), None);
    }

    #[test]
    fn tombstone_hides_newer_but_not_older_snapshot() {
        let mut m = RefModel::new();
        m.put(b"k".to_vec(), b"v1".to_vec(), 1);
        m.delete(b"k", 2);

        assert_eq!(m.get(b"k"), None);
        assert_eq!(m.get_at(b"k", 2), None);
        assert_eq!(
            m.get_at(b"k", 1).map(|v| v.as_slice()),
            Some(b"v1".as_ref())
        );
    }

    #[test]
    fn put_after_delete_is_visible_again() {
        let mut m = RefModel::new();
        m.put(b"k".to_vec(), b"v1".to_vec(), 1);
        m.delete(b"k", 2);
        m.put(b"k".to_vec(), b"v3".to_vec(), 3);

        assert_eq!(m.get(b"k").map(|v| v.as_slice()), Some(b"v3".as_ref()));
        assert_eq!(m.get_at(b"k", 2), None);
        assert_eq!(
            m.get_at(b"k", 1).map(|v| v.as_slice()),
            Some(b"v1".as_ref())
        );
    }

    #[test]
    fn scan_prefix_latest_and_at() {
        let mut m = RefModel::new();
        m.put(b"a1".to_vec(), b"x".to_vec(), 1);
        m.put(b"a2".to_vec(), b"y1".to_vec(), 2);
        m.put(b"a2".to_vec(), b"y2".to_vec(), 3);
        m.put(b"b1".to_vec(), b"z".to_vec(), 4);
        m.delete(b"a1", 5);

        let latest = m.scan_prefix(b"a");
        assert_eq!(
            latest,
            vec![(b"a2".to_vec(), b"y2".to_vec())],
            "a1 deleted, a2 latest y2"
        );

        let at2 = m.scan_prefix_at(b"a", 2);
        assert_eq!(
            at2,
            vec![
                (b"a1".to_vec(), b"x".to_vec()),
                (b"a2".to_vec(), b"y1".to_vec()),
            ]
        );
    }

    #[test]
    fn apply_log_entry_synthetic_seq() {
        let mut m = RefModel::new();
        let put = RaftCommand::Put {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
        }
        .encode();
        m.apply_log_entry(&put).unwrap();
        assert_eq!(m.get(b"k").map(|v| v.as_slice()), Some(b"v".as_ref()));
        assert_eq!(m.versions_of(b"k"), vec![1]);

        let del = RaftCommand::Delete { key: b"k".to_vec() }.encode();
        m.apply_log_entry(&del).unwrap();
        assert_eq!(m.get(b"k"), None);
        assert_eq!(m.versions_of(b"k"), vec![1, 2]);
        assert_eq!(m.get_at(b"k", 1).map(|v| v.as_slice()), Some(b"v".as_ref()));
    }

    #[test]
    fn all_commit_timestamps_sorted_unique() {
        let mut m = RefModel::new();
        m.put(b"a".to_vec(), b"1".to_vec(), 3);
        m.put(b"b".to_vec(), b"2".to_vec(), 1);
        m.put(b"a".to_vec(), b"3".to_vec(), 5);
        m.delete(b"b", 5);
        assert_eq!(m.all_commit_timestamps(), vec![1, 3, 5]);
    }
}
