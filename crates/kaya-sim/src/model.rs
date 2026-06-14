use std::collections::BTreeMap;

use kaya_raft::RaftCommand;

/// Simple in-memory reference model: a `BTreeMap` that tracks the most recent
/// value for every key that has been written.  Deleted keys are removed.
pub struct RefModel {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl RefModel {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.map.insert(key, value);
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.map.remove(key);
    }

    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.map.get(key)
    }

    /// Apply a replicated Raft log entry payload to this state machine.
    /// Empty payloads are leader no-ops and are ignored.
    pub fn apply_log_entry(&mut self, command: &[u8]) -> Result<(), String> {
        if command.is_empty() {
            return Ok(());
        }
        match RaftCommand::decode(command)? {
            RaftCommand::Put { key, value } => {
                self.put(key, value);
                Ok(())
            }
            RaftCommand::Delete { key } => {
                self.delete(&key);
                Ok(())
            }
            RaftCommand::ConfigChange { .. } => Ok(()),
        }
    }

    /// Return all live `(key, value)` pairs whose key starts with `prefix`,
    /// in sorted key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
