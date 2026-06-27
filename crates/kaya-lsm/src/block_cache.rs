use std::collections::HashMap;

use super::SstEntry;

/// Public block-cache counters for a single `SstableReader`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BlockCacheStats {
    pub hits: u64,
    pub misses: u64,
}

/// LRU cache of decoded SSTable data blocks, keyed by `block_offset` within one file.
#[derive(Debug)]
pub(crate) struct BlockCache {
    capacity: usize,
    order: Vec<u64>,
    map: HashMap<u64, Vec<SstEntry>>,
    hits: u64,
    misses: u64,
}

impl BlockCache {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: Vec::new(),
            map: HashMap::new(),
            hits: 0,
            misses: 0,
        }
    }

    pub(crate) fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            hits: self.hits,
            misses: self.misses,
        }
    }

    pub(crate) fn get(&mut self, block_offset: u64) -> Option<Vec<SstEntry>> {
        if !self.map.contains_key(&block_offset) {
            self.misses += 1;
            return None;
        }
        self.hits += 1;
        if let Some(pos) = self.order.iter().position(|k| *k == block_offset) {
            self.order.remove(pos);
        }
        self.order.push(block_offset);
        self.map.get(&block_offset).cloned()
    }

    pub(crate) fn insert(&mut self, block_offset: u64, entries: Vec<SstEntry>) {
        if self.capacity == 0 {
            return;
        }
        if self.map.contains_key(&block_offset) {
            if let Some(pos) = self.order.iter().position(|k| *k == block_offset) {
                self.order.remove(pos);
            }
        } else if self.map.len() >= self.capacity {
            if let Some(evict) = self.order.first().copied() {
                self.order.remove(0);
                self.map.remove(&evict);
            }
        }
        self.map.insert(block_offset, entries);
        self.order.push(block_offset);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_core::SequenceNumber;

    fn entry(key: &[u8]) -> SstEntry {
        SstEntry {
            key: key.to_vec(),
            value: Some(b"v".to_vec()),
            sequence: SequenceNumber::new(1),
        }
    }

    #[test]
    fn lru_evicts_oldest() {
        let mut cache = BlockCache::new(2);
        cache.insert(1, vec![entry(b"a")]);
        cache.insert(2, vec![entry(b"b")]);
        assert!(cache.get(1).is_some());
        cache.insert(3, vec![entry(b"c")]);
        assert!(cache.get(2).is_none());
        assert!(cache.get(1).is_some());
        assert!(cache.get(3).is_some());
    }

    #[test]
    fn stats_track_hits_and_misses() {
        let mut cache = BlockCache::new(4);
        cache.insert(1, vec![entry(b"a")]);
        assert_eq!(cache.stats().misses, 0);
        assert!(cache.get(1).is_some());
        assert_eq!(cache.stats().hits, 1);
        assert!(cache.get(99).is_none());
        assert_eq!(cache.stats().misses, 1);
    }
}