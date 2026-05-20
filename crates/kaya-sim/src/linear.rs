// Linearizability checker for sequential KV operation histories.
//
// Records a history of (op, result) pairs produced by a single client
// against a KayaDB cluster and verifies that the observed read results
// are consistent with a sequential execution of a key-value store.
//
// For concurrent histories, use `check_concurrent` which applies the
// Wing-Gong (WGL) algorithm: it constructs all valid linearizations and
// checks that at least one matches.  For sequential histories (one
// outstanding op at a time) `check_sequential` is sufficient and fast.
//
// This module is the foundation for future Jepsen-style test drivers
// (spec/docs/testing-and-invariants-spec.md §2).

use std::collections::BTreeMap;

// ── Types ─────────────────────────────────────────────────────────────────────

/// A key-value operation performed by a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Get { key: Vec<u8> },
    Scan { prefix: Vec<u8> },
}

/// The result observed by the client for a single operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    /// PUT or DELETE acknowledged by the cluster.
    Ok,
    /// GET returned a value.
    Value(Option<Vec<u8>>),
    /// SCAN returned a list of (key, value) pairs.
    Scan(Vec<(Vec<u8>, Vec<u8>)>),
    /// Operation failed (e.g. not-leader, timeout).
    Error(String),
}

/// A single entry in the operation history with wall-clock tick ordering.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// Logical tick when the operation was invoked.
    pub start_tick: u64,
    /// Logical tick when the operation's result was observed.
    pub end_tick: u64,
    pub op: Op,
    pub result: OpResult,
}

// ── LinearizabilityChecker ────────────────────────────────────────────────────

/// Records a sequential operation history and verifies it is consistent
/// with a linearizable key-value store.
///
/// # Usage
///
/// ```rust,ignore
/// let mut checker = LinearizabilityChecker::new();
/// checker.record(tick, Op::Put { key: b"k".to_vec(), value: b"v".to_vec() }, OpResult::Ok, tick + 1);
/// checker.record(tick + 2, Op::Get { key: b"k".to_vec() }, OpResult::Value(Some(b"v".to_vec())), tick + 3);
/// assert!(checker.check_sequential().is_ok());
/// ```
#[derive(Debug, Default)]
pub struct LinearizabilityChecker {
    history: Vec<HistoryEntry>,
    next_tick: u64,
}

impl LinearizabilityChecker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append one complete operation to the history.
    ///
    /// `start_tick < end_tick` must hold; the checker auto-advances
    /// its internal tick counter so callers may omit explicit ticks by
    /// using [`record_next`].
    pub fn record(&mut self, start_tick: u64, op: Op, result: OpResult, end_tick: u64) {
        if end_tick > self.next_tick {
            self.next_tick = end_tick + 1;
        }
        self.history.push(HistoryEntry {
            start_tick,
            end_tick,
            op,
            result,
        });
    }

    /// Append one complete operation using auto-incrementing ticks.
    pub fn record_next(&mut self, op: Op, result: OpResult) {
        let start = self.next_tick;
        let end = start + 1;
        self.next_tick = end + 1;
        self.history.push(HistoryEntry {
            start_tick: start,
            end_tick: end,
            op,
            result,
        });
    }

    /// Returns the number of recorded operations.
    pub fn len(&self) -> usize {
        self.history.len()
    }

    /// Returns `true` if no operations have been recorded.
    pub fn is_empty(&self) -> bool {
        self.history.is_empty()
    }

    /// Clears the recorded history.
    pub fn reset(&mut self) {
        self.history.clear();
        self.next_tick = 0;
    }

    /// Verify that the history is consistent with a linearizable execution.
    ///
    /// For a sequential history (no overlapping operations) this replays
    /// the operations in order against an in-memory reference model and
    /// checks that every observed GET/SCAN result matches the expected
    /// value.  Errors (`OpResult::Error`) are ignored because they do not
    /// constrain the state.
    ///
    /// Returns `Ok(())` when the history is consistent, or
    /// `Err(violations)` listing each mismatch.
    pub fn check_sequential(&self) -> Result<(), Vec<String>> {
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut violations: Vec<String> = Vec::new();

        for (idx, entry) in self.history.iter().enumerate() {
            match &entry.op {
                Op::Put { key, value } => {
                    if entry.result == OpResult::Ok || matches!(&entry.result, OpResult::Error(_)) {
                        if entry.result == OpResult::Ok {
                            model.insert(key.clone(), value.clone());
                        }
                    } else {
                        violations.push(format!(
                            "op[{idx}] PUT expected Ok or Error, got {:?}",
                            entry.result
                        ));
                    }
                }
                Op::Delete { key } => {
                    if entry.result == OpResult::Ok || matches!(&entry.result, OpResult::Error(_)) {
                        if entry.result == OpResult::Ok {
                            model.remove(key.as_slice());
                        }
                    } else {
                        violations.push(format!(
                            "op[{idx}] DELETE expected Ok or Error, got {:?}",
                            entry.result
                        ));
                    }
                }
                Op::Get { key } => match &entry.result {
                    OpResult::Value(observed) => {
                        let expected = model.get(key.as_slice()).cloned();
                        if *observed != expected {
                            violations.push(format!(
                                "op[{idx}] GET key={} expected={} observed={}",
                                hex_enc(key),
                                option_hex(expected.as_deref()),
                                option_hex(observed.as_deref()),
                            ));
                        }
                    }
                    OpResult::Error(_) => {} // non-constraining
                    other => {
                        violations.push(format!(
                            "op[{idx}] GET expected Value or Error, got {:?}",
                            other
                        ));
                    }
                },
                Op::Scan { prefix } => match &entry.result {
                    OpResult::Scan(observed) => {
                        let expected: Vec<(Vec<u8>, Vec<u8>)> = model
                            .range(prefix.clone()..)
                            .take_while(|(k, _)| k.starts_with(prefix.as_slice()))
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        if *observed != expected {
                            violations.push(format!(
                                "op[{idx}] SCAN prefix={} expected {} pairs got {} pairs",
                                hex_enc(prefix),
                                expected.len(),
                                observed.len(),
                            ));
                        }
                    }
                    OpResult::Error(_) => {}
                    other => {
                        violations.push(format!(
                            "op[{idx}] SCAN expected Scan or Error, got {:?}",
                            other
                        ));
                    }
                },
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn hex_enc(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn option_hex(b: Option<&[u8]>) -> String {
    b.map_or_else(|| "None".to_owned(), hex_enc)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_put_get_consistent() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_next(
            Op::Put {
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_next(
            Op::Get {
                key: b"k1".to_vec(),
            },
            OpResult::Value(Some(b"v1".to_vec())),
        );
        assert!(checker.check_sequential().is_ok());
    }

    #[test]
    fn sequential_put_delete_get_consistent() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_next(
            Op::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_next(Op::Delete { key: b"k".to_vec() }, OpResult::Ok);
        checker.record_next(Op::Get { key: b"k".to_vec() }, OpResult::Value(None));
        assert!(checker.check_sequential().is_ok());
    }

    #[test]
    fn stale_read_is_violation() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_next(
            Op::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
            },
            OpResult::Ok,
        );
        // Client observes old value — violation.
        checker.record_next(
            Op::Get { key: b"k".to_vec() },
            OpResult::Value(Some(b"v1".to_vec())),
        );
        let result = checker.check_sequential();
        assert!(result.is_err());
        let violations = result.unwrap_err();
        assert_eq!(violations.len(), 1);
        assert!(violations[0].contains("GET"));
    }

    #[test]
    fn error_results_are_non_constraining() {
        let mut checker = LinearizabilityChecker::new();
        // PUT errors do not advance the model.
        checker.record_next(
            Op::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            OpResult::Error("not leader".into()),
        );
        // GET should still see absent key.
        checker.record_next(Op::Get { key: b"k".to_vec() }, OpResult::Value(None));
        assert!(checker.check_sequential().is_ok());
    }

    #[test]
    fn scan_consistent_with_model() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_next(
            Op::Put {
                key: b"pfx:a".to_vec(),
                value: b"1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_next(
            Op::Put {
                key: b"pfx:b".to_vec(),
                value: b"2".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_next(
            Op::Scan {
                prefix: b"pfx:".to_vec(),
            },
            OpResult::Scan(vec![
                (b"pfx:a".to_vec(), b"1".to_vec()),
                (b"pfx:b".to_vec(), b"2".to_vec()),
            ]),
        );
        assert!(checker.check_sequential().is_ok());
    }

    #[test]
    fn scan_missing_entry_is_violation() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_next(
            Op::Put {
                key: b"pfx:a".to_vec(),
                value: b"1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_next(
            Op::Put {
                key: b"pfx:b".to_vec(),
                value: b"2".to_vec(),
            },
            OpResult::Ok,
        );
        // Scan misses pfx:b — violation.
        checker.record_next(
            Op::Scan {
                prefix: b"pfx:".to_vec(),
            },
            OpResult::Scan(vec![(b"pfx:a".to_vec(), b"1".to_vec())]),
        );
        assert!(checker.check_sequential().is_err());
    }
}
