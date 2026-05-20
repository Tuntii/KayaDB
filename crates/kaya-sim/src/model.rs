use std::collections::BTreeMap;

/// Simple in-memory reference model: a `BTreeMap` that tracks the most recent
/// value for every key that has been written.  Deleted keys are removed.
pub(crate) struct RefModel {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl RefModel {
    pub(crate) fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub(crate) fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.map.insert(key, value);
    }

    pub(crate) fn delete(&mut self, key: &[u8]) {
        self.map.remove(key);
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        self.map.get(key)
    }

    /// Return all live `(key, value)` pairs whose key starts with `prefix`,
    /// in sorted key order.
    pub(crate) fn scan_prefix(&self, prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.map
            .range(prefix.to_vec()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}
