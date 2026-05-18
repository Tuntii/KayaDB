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
        self.entries
            .insert(key, ValueRecord::Put { value, sequence });
        self.recompute_approximate_bytes();
    }

    pub fn delete(&mut self, key: Bytes, sequence: SequenceNumber) {
        self.entries.insert(key, ValueRecord::Delete { sequence });
        self.recompute_approximate_bytes();
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
        for (key, value) in self.entries.range(prefix.to_vec()..) {
            if !key.starts_with(prefix) {
                break;
            }
            if let ValueRecord::Put { value, .. } = value {
                items.push(KeyValue {
                    key: key.clone(),
                    value: value.clone(),
                });
            }
        }
        items
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

    fn recompute_approximate_bytes(&mut self) {
        self.approximate_bytes = self
            .entries
            .iter()
            .map(|(key, record)| {
                key.len()
                    + match record {
                        ValueRecord::Put { value, .. } => value.len(),
                        ValueRecord::Delete { .. } => 0,
                    }
            })
            .sum();
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
}
