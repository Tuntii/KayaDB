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

    /// Record a completed operation.
    pub fn record(&self, client_id: usize, op: Op, result: OperationResult) {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let now = Instant::now();
        self.operations.lock().unwrap().push(Operation {
            id,
            client_id,
            op,
            result,
            start_time: now, // Simplified: use same time for start/end
            end_time: now,
        });
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
