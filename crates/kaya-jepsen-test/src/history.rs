//! Operation history recording for linearizability checking.

use kaya_sim::{LinearizabilityChecker, Op, OpResult};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

/// A single operation in the history.
#[derive(Debug, Clone)]
pub struct Operation {
    /// Unique operation ID
    pub id: u64,
    /// Client ID that performed the operation
    pub client_id: usize,
    /// Operation type and arguments
    pub op: Op,
    /// Operation result
    pub result: OperationResult,
    /// Wall-clock time when the operation started
    pub start_time: Instant,
    /// Wall-clock time when the operation completed
    pub end_time: Instant,
}

/// Result of an operation.
#[derive(Debug, Clone)]
pub enum OperationResult {
    /// Operation succeeded
    Ok,
    /// GET returned a value
    Value(Option<Vec<u8>>),
    /// SCAN returned items
    Scan(Vec<(Vec<u8>, Vec<u8>)>),
    /// Operation failed
    Error(String),
}

/// Thread-safe operation history recorder.
pub struct History {
    operations: Mutex<Vec<Operation>>,
    next_id: AtomicU64,
    start_time: Instant,
}

impl History {
    /// Create a new empty history.
    pub fn new() -> Self {
        Self {
            operations: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(1),
            start_time: Instant::now(),
        }
    }

    /// Record a completed operation (start and end times set to the same instant).
    pub fn record(&self, client_id: usize, op: Op, result: OperationResult) {
        let now = Instant::now();
        self.record_timed(client_id, op, result, now, now);
    }

    /// Record a completed operation with explicit wall-clock interval.
    pub fn record_timed(
        &self,
        client_id: usize,
        op: Op,
        result: OperationResult,
        start_time: Instant,
        end_time: Instant,
    ) {
        let _ = self.try_record_timed(None, client_id, op, result, start_time, end_time);
    }

    /// Record when under an optional cap; returns false if the cap is already reached.
    pub fn try_record_timed(
        &self,
        max_ops: Option<usize>,
        client_id: usize,
        op: Op,
        result: OperationResult,
        start_time: Instant,
        end_time: Instant,
    ) -> bool {
        let mut ops = self.operations.lock().unwrap();
        if max_ops.is_some_and(|max| ops.len() >= max) {
            return false;
        }
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        ops.push(Operation {
            id,
            client_id,
            op,
            result,
            start_time,
            end_time,
        });
        true
    }

    /// Get the number of recorded operations.
    pub fn len(&self) -> usize {
        self.operations.lock().unwrap().len()
    }

    /// Check if history is empty.
    pub fn is_empty(&self) -> bool {
        self.operations.lock().unwrap().is_empty()
    }

    /// Verify linearizability using sequential checker.
    ///
    /// Returns Ok(()) if the history is linearizable, or Err with violations.
    pub fn check_linearizability(&self) -> Result<(), Vec<String>> {
        let ops = self.operations.lock().unwrap();
        let mut checker = LinearizabilityChecker::new();

        for op in ops.iter() {
            let sim_result = match &op.result {
                OperationResult::Ok => OpResult::Ok,
                OperationResult::Value(v) => OpResult::Value(v.clone()),
                OperationResult::Scan(items) => OpResult::Scan(items.clone()),
                OperationResult::Error(e) => OpResult::Error(e.clone()),
            };
            checker.record_next(op.op.clone(), sim_result);
        }

        checker.check_sequential()
    }

    /// Verify linearizability of a concurrent history (WGL algorithm).
    ///
    /// Uses wall-clock `start_time`/`end_time` on each operation, converted to
    /// logical ticks for overlap detection.
    pub fn check_concurrent(&self) -> Result<(), Vec<String>> {
        let ops = self.operations.lock().unwrap();
        if ops.is_empty() {
            return Ok(());
        }

        let base = ops
            .iter()
            .map(|op| op.start_time)
            .min()
            .unwrap_or(self.start_time);

        let mut key_partitions: std::collections::BTreeMap<Vec<u8>, LinearizabilityChecker> =
            std::collections::BTreeMap::new();
        let mut scan_checker = LinearizabilityChecker::new();
        let mut scan_ops = 0usize;

        for op in ops.iter() {
            let start_tick = op.start_time.duration_since(base).as_micros() as u64;
            let end_tick = op.end_time.duration_since(base).as_micros() as u64;
            let sim_result = match &op.result {
                OperationResult::Ok => OpResult::Ok,
                OperationResult::Value(v) => OpResult::Value(v.clone()),
                OperationResult::Scan(items) => OpResult::Scan(items.clone()),
                OperationResult::Error(e) => OpResult::Error(e.clone()),
            };
            let interval = (
                start_tick,
                end_tick.max(start_tick + 1),
                Some(op.client_id as u32),
                op.op.clone(),
                sim_result,
            );

            match &op.op {
                Op::Put { key, .. } | Op::Get { key } | Op::Delete { key } => {
                    key_partitions
                        .entry(key.clone())
                        .or_default()
                        .record_interval(
                            interval.0, interval.1, interval.2, interval.3, interval.4,
                        );
                }
                Op::Scan { .. } => {
                    scan_ops += 1;
                    scan_checker.record_interval(
                        interval.0, interval.1, interval.2, interval.3, interval.4,
                    );
                }
            }
        }

        let mut violations = Vec::new();
        for (key, checker) in key_partitions {
            if let Err(mut v) = checker.check_concurrent() {
                for item in &mut v {
                    *item = format!("key {:?}: {item}", String::from_utf8_lossy(&key));
                }
                violations.append(&mut v);
            }
        }
        if scan_ops > 0 {
            if let Err(mut v) = scan_checker.check_concurrent() {
                for item in &mut v {
                    *item = format!("scan ops: {item}");
                }
                violations.append(&mut v);
            }
        }

        if violations.is_empty() {
            Ok(())
        } else {
            Err(violations)
        }
    }

    /// Export history as JSONL trace.
    pub fn to_trace(&self, seed: u64) -> String {
        let ops = self.operations.lock().unwrap();
        let mut checker = LinearizabilityChecker::new();

        for op in ops.iter() {
            let sim_result = match &op.result {
                OperationResult::Ok => OpResult::Ok,
                OperationResult::Value(v) => OpResult::Value(v.clone()),
                OperationResult::Scan(items) => OpResult::Scan(items.clone()),
                OperationResult::Error(e) => OpResult::Error(e.clone()),
            };
            checker.record_next(op.op.clone(), sim_result);
        }

        checker.to_trace_string(seed)
    }

    /// Get summary statistics.
    pub fn stats(&self) -> HistoryStats {
        let ops = self.operations.lock().unwrap();
        let mut puts = 0;
        let mut gets = 0;
        let mut deletes = 0;
        let mut scans = 0;
        let mut errors = 0;

        for op in ops.iter() {
            match &op.op {
                Op::Put { .. } => puts += 1,
                Op::Get { .. } => gets += 1,
                Op::Delete { .. } => deletes += 1,
                Op::Scan { .. } => scans += 1,
            }
            if matches!(&op.result, OperationResult::Error(_)) {
                errors += 1;
            }
        }

        HistoryStats {
            total: ops.len(),
            puts,
            gets,
            deletes,
            scans,
            errors,
            duration_ms: self.start_time.elapsed().as_millis() as u64,
        }
    }
}

impl Default for History {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary statistics for a history.
#[derive(Debug, Clone)]
pub struct HistoryStats {
    pub total: usize,
    pub puts: usize,
    pub gets: usize,
    pub deletes: usize,
    pub scans: usize,
    pub errors: usize,
    pub duration_ms: u64,
}

impl std::fmt::Display for HistoryStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "History: {} ops ({} PUT, {} GET, {} DEL, {} SCAN, {} errors) in {}ms",
            self.total,
            self.puts,
            self.gets,
            self.deletes,
            self.scans,
            self.errors,
            self.duration_ms
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_sim::Op;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn check_concurrent_accepts_overlapping_puts() {
        let history = History::new();
        let t0 = Instant::now();
        history.record_timed(
            0,
            Op::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
            OperationResult::Ok,
            t0,
            t0 + Duration::from_micros(10),
        );
        thread::sleep(Duration::from_micros(5));
        let t1 = Instant::now();
        history.record_timed(
            1,
            Op::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
            },
            OperationResult::Ok,
            t1,
            t1 + Duration::from_micros(10),
        );
        let t2 = Instant::now();
        history.record_timed(
            0,
            Op::Get { key: b"k".to_vec() },
            OperationResult::Value(Some(b"v2".to_vec())),
            t2,
            t2 + Duration::from_micros(5),
        );
        assert!(history.check_concurrent().is_ok());
    }
}

// (Display impl moved earlier in the file before the tests module)
