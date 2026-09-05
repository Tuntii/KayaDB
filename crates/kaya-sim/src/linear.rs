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
// When concurrent check fails, `minimal_counterexample` greedily shrinks the
// history to a small non-linearizable subset for operator diagnosis.
// `minimal_unsatisfiable_subsets` enumerates all inclusion-minimal failing
// subsets (MUSs) under an op cap (WGL bound, default 14).
//
// This module is the foundation for future Jepsen-style test drivers
// (spec/docs/testing-and-invariants-spec.md §2).

use std::collections::BTreeMap;
use std::fmt;

/// Maximum history length [`LinearizabilityChecker::check_concurrent`] will search.
///
/// WGL is exponential in the number of overlapping ops; Jepsen full-gate
/// recording uses the same bound (`kaya-jepsen-test::WGL_VERIFY_MAX_OPS`).
pub const WGL_MAX_OPS: usize = 14;

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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    /// Logical tick when the operation was invoked.
    pub start_tick: u64,
    /// Logical tick when the operation's result was observed.
    pub end_tick: u64,
    /// Optional client identifier for concurrent histories.
    pub client_id: Option<u32>,
    pub op: Op,
    pub result: OpResult,
}

/// Compact non-linearizable subset of a concurrent history (WGL residual).
///
/// Produced when the full history has no valid linearization; the subset is
/// greedily minimal (no single op can be dropped while remaining illegal).
#[derive(Debug, Clone)]
pub struct MinimalCounterexample {
    /// Indices into the original history (stable order).
    pub original_indices: Vec<usize>,
    /// The ops that form the counterexample (same order as `original_indices`).
    pub ops: Vec<HistoryEntry>,
    /// Why this subset fails (last WGL failure strings).
    pub why: Vec<String>,
}

impl MinimalCounterexample {
    /// Human-readable multi-line report for logs / CI.
    pub fn report(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "minimal counterexample: {} op(s) (indices {:?})",
            self.ops.len(),
            self.original_indices
        ));
        for (i, (orig, e)) in self
            .original_indices
            .iter()
            .zip(self.ops.iter())
            .enumerate()
        {
            lines.push(format!(
                "  [{i}] orig={orig} client={} [{},{}) {} → {}",
                e.client_id
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "-".into()),
                e.start_tick,
                e.end_tick,
                format_op(&e.op),
                format_result(&e.result),
            ));
        }
        // Real-time precedence edges within the subset.
        let mut edges = Vec::new();
        for (i, a) in self.ops.iter().enumerate() {
            for (j, b) in self.ops.iter().enumerate() {
                if i != j && a.end_tick < b.start_tick {
                    edges.push(format!("{i}≺{j}"));
                }
            }
        }
        if edges.is_empty() {
            lines.push("  real-time order: (none — all intervals overlap)".into());
        } else {
            lines.push(format!("  real-time order: {}", edges.join(", ")));
        }
        if !self.why.is_empty() {
            lines.push(format!("  reason: {}", self.why.join("; ")));
        } else {
            lines.push("  reason: no linearization extends the real-time order".into());
        }
        lines.join("\n")
    }
}

impl fmt::Display for MinimalCounterexample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.report())
    }
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
            client_id: None,
            op,
            result,
        });
    }

    /// Append one complete operation using auto-incrementing ticks.
    pub fn record_next(&mut self, op: Op, result: OpResult) {
        self.record_next_with_client(op, result, None);
    }

    /// Append with explicit client id and tick interval (for concurrent histories).
    pub fn record_interval(
        &mut self,
        start_tick: u64,
        end_tick: u64,
        client_id: Option<u32>,
        op: Op,
        result: OpResult,
    ) {
        if end_tick > self.next_tick {
            self.next_tick = end_tick + 1;
        }
        self.history.push(HistoryEntry {
            start_tick,
            end_tick,
            client_id,
            op,
            result,
        });
    }

    /// Append using auto ticks with optional client id.
    pub fn record_next_with_client(&mut self, op: Op, result: OpResult, client_id: Option<u32>) {
        let start = self.next_tick;
        let end = start + 1;
        self.next_tick = end + 1;
        self.history.push(HistoryEntry {
            start_tick: start,
            end_tick: end,
            client_id,
            op,
            result,
        });
    }

    /// Returns the number of recorded operations.
    pub fn len(&self) -> usize {
        self.history.len()
    }

    /// Recorded operations in insertion order.
    pub fn entries(&self) -> &[HistoryEntry] {
        &self.history
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

    /// Verify a concurrent history using the Wing-Gong linearization search.
    ///
    /// Operations `i` and `j` where `i.end_tick < j.start_tick` must appear before `j`
    /// in any valid linearization. Returns `Ok(())` when at least one linearization
    /// matches all observed GET/SCAN results.
    pub fn check_concurrent(&self) -> Result<(), Vec<String>> {
        if self.history.len() > WGL_MAX_OPS {
            return Err(vec![format!(
                "concurrent check supports at most {WGL_MAX_OPS} ops (have {})",
                self.history.len()
            )]);
        }
        if self.history.is_empty() {
            return Ok(());
        }

        // Non-overlapping histories can use the fast sequential path.
        let mut sequential = true;
        for i in 0..self.history.len() {
            for j in (i + 1)..self.history.len() {
                let a = &self.history[i];
                let b = &self.history[j];
                if a.start_tick < b.end_tick && b.start_tick < a.end_tick {
                    sequential = false;
                    break;
                }
            }
            if !sequential {
                break;
            }
        }
        if sequential {
            return self.check_sequential();
        }

        let n = self.history.len();
        let mut must_before = vec![vec![false; n]; n];
        for (i, hi) in self.history.iter().enumerate() {
            for (j, hj) in self.history.iter().enumerate() {
                if i != j && hi.end_tick < hj.start_tick {
                    must_before[i][j] = true;
                }
            }
        }

        let mut order = Vec::with_capacity(n);
        let mut used = vec![false; n];
        let mut found = false;
        let mut last_error: Vec<String> = Vec::new();

        fn dfs(
            history: &[HistoryEntry],
            must_before: &[Vec<bool>],
            order: &mut Vec<usize>,
            used: &mut [bool],
            found: &mut bool,
            last_error: &mut Vec<String>,
        ) {
            if *found {
                return;
            }
            if order.len() == history.len() {
                match verify_linearization(history, order) {
                    Ok(()) => *found = true,
                    Err(e) => *last_error = e,
                }
                return;
            }
            for i in 0..history.len() {
                if used[i] {
                    continue;
                }
                let blocked = order.iter().any(|&j| must_before[j][i]);
                if blocked {
                    continue;
                }
                used[i] = true;
                order.push(i);
                dfs(history, must_before, order, used, found, last_error);
                order.pop();
                used[i] = false;
                if *found {
                    return;
                }
            }
        }

        dfs(
            &self.history,
            &must_before,
            &mut order,
            &mut used,
            &mut found,
            &mut last_error,
        );

        if found {
            Ok(())
        } else if last_error.is_empty() {
            Err(vec![
                "no linearization extends the real-time order".to_owned()
            ])
        } else {
            Err(last_error)
        }
    }

    /// Like [`check_concurrent`], but on failure also returns a greedily-minimal
    /// non-linearizable subset for diagnosis.
    pub fn check_concurrent_detailed(
        &self,
    ) -> Result<(), (Vec<String>, Option<MinimalCounterexample>)> {
        match self.check_concurrent() {
            Ok(()) => Ok(()),
            Err(violations) => {
                let cex = self.minimal_counterexample();
                Err((violations, cex))
            }
        }
    }

    /// If the history is non-linearizable under WGL, return a greedily minimal
    /// non-linearizable subset (no single remaining op can be dropped while still
    /// illegal). Returns `None` when the full history is linearizable.
    ///
    /// Multi-key histories are first partitioned by key (and scans separately),
    /// matching Jepsen's per-key WGL checks, so shrink stays inside the failing
    /// partition and does not invent unrelated single-op failures on other keys.
    ///
    /// Complexity is acceptable under the WGL op cap (≤[`WGL_MAX_OPS`]).
    pub fn minimal_counterexample(&self) -> Option<MinimalCounterexample> {
        if self.check_concurrent().is_ok() {
            return None;
        }

        let (by_key, scan_idxs) = key_and_scan_partitions(&self.history);
        let mut best: Option<MinimalCounterexample> = None;

        for (_key, idxs) in by_key {
            if let Some(cex) = shrink_indices_to_minimal(&self.history, &idxs) {
                best = Some(pick_smaller(best, cex));
            }
        }
        if !scan_idxs.is_empty() {
            if let Some(cex) = shrink_indices_to_minimal(&self.history, &scan_idxs) {
                best = Some(pick_smaller(best, cex));
            }
        }

        // Fallback: whole-history shrink (single partition / mixed).
        if best.is_none() {
            let all: Vec<usize> = (0..self.history.len()).collect();
            best = shrink_indices_to_minimal(&self.history, &all);
        }
        best
    }

    /// Enumerate inclusion-minimal non-linearizable subsets (MUSs).
    ///
    /// A MUS is a failing subset such that dropping any one op makes it
    /// linearizable. Unlike [`minimal_counterexample`], this returns every such
    /// subset, not only the greedy one.
    ///
    /// `cap` is the maximum universe size to brute-force (clamped to
    /// [`WGL_MAX_OPS`]). Histories at most `cap` ops are enumerated in full.
    /// Larger histories are partitioned by key (and scan-related ops) the same
    /// way as greedy shrink; partitions bigger than `cap` are skipped.
    ///
    /// Returns an empty vec when the history is linearizable, or when every
    /// failing partition exceeds `cap`.
    pub fn minimal_unsatisfiable_subsets(&self, cap: usize) -> Vec<MinimalCounterexample> {
        let cap = cap.min(WGL_MAX_OPS);
        if cap == 0 || self.history.is_empty() {
            return Vec::new();
        }

        let mut found: Vec<MinimalCounterexample> = Vec::new();

        if self.history.len() <= cap {
            let all: Vec<usize> = (0..self.history.len()).collect();
            found = enumerate_mus_in(&self.history, &all, cap);
            sort_mus(&mut found);
            return found;
        }

        let (by_key, scan_idxs) = key_and_scan_partitions(&self.history);
        for idxs in by_key.values() {
            for cex in enumerate_mus_in(&self.history, idxs, cap) {
                push_unique_cex(&mut found, cex);
            }
        }
        if !scan_idxs.is_empty() {
            for cex in enumerate_mus_in(&self.history, &scan_idxs, cap) {
                push_unique_cex(&mut found, cex);
            }
            let mixed = scan_related_indices(&self.history);
            if mixed.len() <= cap {
                for cex in enumerate_mus_in(&self.history, &mixed, cap) {
                    push_unique_cex(&mut found, cex);
                }
            }
        }

        sort_mus(&mut found);
        found
    }

    /// Build a checker from an explicit entry list (preserves ticks/clients).
    fn from_entries(entries: Vec<HistoryEntry>) -> Self {
        let next_tick = entries
            .iter()
            .map(|e| e.end_tick.saturating_add(1))
            .max()
            .unwrap_or(0);
        Self {
            history: entries,
            next_tick,
        }
    }

    /// Convert the recorded history into a JSONL trace string compatible with simulation replayer.
    pub fn to_trace_string(&self, seed: u64) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            r#"{{"eid":1,"kind":"sim_start","seed":"0x{seed:016x}","max_ops":{}}}"#,
            self.history.len()
        ));
        let mut eid = 2;
        for entry in &self.history {
            let oid = eid / 2;
            match &entry.op {
                Op::Put { key, value } => {
                    let k = hex_enc(key);
                    let v = hex_enc(value);
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op","oid":{oid},"cmd":"put","key":"{k}","val":"{v}"}}"#
                    ));
                }
                Op::Get { key } => {
                    let k = hex_enc(key);
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op","oid":{oid},"cmd":"get","key":"{k}"}}"#
                    ));
                }
                Op::Delete { key } => {
                    let k = hex_enc(key);
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op","oid":{oid},"cmd":"delete","key":"{k}"}}"#
                    ));
                }
                Op::Scan { prefix } => {
                    let p = hex_enc(prefix);
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op","oid":{oid},"cmd":"scan","prefix":"{p}"}}"#
                    ));
                }
            }
            eid += 1;

            match &entry.result {
                OpResult::Ok => {
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op_result","oid":{oid},"ok":true}}"#
                    ));
                }
                OpResult::Value(val) => match val {
                    Some(v) => {
                        let vh = hex_enc(v);
                        lines.push(format!(
                                r#"{{"eid":{eid},"kind":"op_result","oid":{oid},"ok":true,"val":"{vh}"}}"#
                            ));
                    }
                    None => {
                        lines.push(format!(
                            r#"{{"eid":{eid},"kind":"op_result","oid":{oid},"ok":true,"val":null}}"#
                        ));
                    }
                },
                OpResult::Scan(items) => {
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op_result","oid":{oid},"ok":true,"count":{}}}"#,
                        items.len()
                    ));
                }
                OpResult::Error(err) => {
                    let safe = err.replace('"', "'");
                    lines.push(format!(
                        r#"{{"eid":{eid},"kind":"op_result","oid":{oid},"ok":false,"error":"{safe}"}}"#
                    ));
                }
            }
            eid += 1;
        }
        lines.join("\n")
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn verify_linearization(history: &[HistoryEntry], order: &[usize]) -> Result<(), Vec<String>> {
    let ordered: Vec<&HistoryEntry> = order.iter().map(|&i| &history[i]).collect();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut violations = Vec::new();

    for (idx, entry) in ordered.iter().enumerate() {
        match &entry.op {
            Op::Put { key, value } => {
                if entry.result == OpResult::Ok {
                    model.insert(key.clone(), value.clone());
                }
            }
            Op::Delete { key } => {
                if entry.result == OpResult::Ok {
                    model.remove(key.as_slice());
                }
            }
            Op::Get { key } => {
                if let OpResult::Value(observed) = &entry.result {
                    let expected = model.get(key.as_slice()).cloned();
                    if *observed != expected {
                        violations.push(format!(
                            "lin[{idx}] GET key={} expected={} observed={}",
                            hex_enc(key),
                            option_hex(expected.as_deref()),
                            option_hex(observed.as_deref()),
                        ));
                    }
                }
            }
            Op::Scan { prefix } => {
                if let OpResult::Scan(observed) = &entry.result {
                    let expected: Vec<(Vec<u8>, Vec<u8>)> = model
                        .range(prefix.clone()..)
                        .take_while(|(k, _)| k.starts_with(prefix.as_slice()))
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    if *observed != expected {
                        violations.push(format!(
                            "lin[{idx}] SCAN prefix={} expected {} pairs got {}",
                            hex_enc(prefix),
                            expected.len(),
                            observed.len(),
                        ));
                    }
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

pub(crate) fn hex_enc(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn option_hex(b: Option<&[u8]>) -> String {
    b.map_or_else(|| "None".to_owned(), hex_enc)
}

fn keep_entries(history: &[HistoryEntry], indices: &[usize]) -> Vec<HistoryEntry> {
    indices.iter().map(|&i| history[i].clone()).collect()
}

/// Greedy minimal non-linearizable subset of `seed` indices into `history`.
fn shrink_indices_to_minimal(
    history: &[HistoryEntry],
    seed: &[usize],
) -> Option<MinimalCounterexample> {
    if seed.is_empty() {
        return None;
    }
    let sub0 = LinearizabilityChecker::from_entries(keep_entries(history, seed));
    if sub0.check_concurrent().is_ok() {
        return None;
    }
    let mut keep = seed.to_vec();
    let mut why = sub0.check_concurrent().err().unwrap_or_default();

    let mut changed = true;
    while changed && keep.len() > 1 {
        changed = false;
        for pos in 0..keep.len() {
            let mut candidate = keep.clone();
            candidate.remove(pos);
            let sub = LinearizabilityChecker::from_entries(keep_entries(history, &candidate));
            if let Err(e) = sub.check_concurrent() {
                why = e;
                keep = candidate;
                changed = true;
                break;
            }
        }
    }
    // Reverse pass for removal-order sensitivity.
    loop {
        let before = keep.len();
        for pos in (0..keep.len()).rev() {
            if keep.len() <= 1 {
                break;
            }
            let mut candidate = keep.clone();
            candidate.remove(pos);
            let sub = LinearizabilityChecker::from_entries(keep_entries(history, &candidate));
            if let Err(e) = sub.check_concurrent() {
                why = e;
                keep = candidate;
            }
        }
        if keep.len() == before {
            break;
        }
    }

    Some(MinimalCounterexample {
        original_indices: keep.clone(),
        ops: keep_entries(history, &keep),
        why,
    })
}

fn pick_smaller(
    best: Option<MinimalCounterexample>,
    cand: MinimalCounterexample,
) -> MinimalCounterexample {
    match best {
        None => cand,
        Some(b) if cand.ops.len() < b.ops.len() => cand,
        Some(b) => b,
    }
}

fn key_and_scan_partitions(
    history: &[HistoryEntry],
) -> (BTreeMap<Vec<u8>, Vec<usize>>, Vec<usize>) {
    let mut by_key: BTreeMap<Vec<u8>, Vec<usize>> = BTreeMap::new();
    let mut scan_idxs: Vec<usize> = Vec::new();
    for (i, e) in history.iter().enumerate() {
        match &e.op {
            Op::Put { key, .. } | Op::Get { key } | Op::Delete { key } => {
                by_key.entry(key.clone()).or_default().push(i);
            }
            Op::Scan { .. } => scan_idxs.push(i),
        }
    }
    (by_key, scan_idxs)
}

/// Scans plus KV ops whose keys fall under at least one scan prefix.
fn scan_related_indices(history: &[HistoryEntry]) -> Vec<usize> {
    let prefixes: Vec<&[u8]> = history
        .iter()
        .filter_map(|e| match &e.op {
            Op::Scan { prefix } => Some(prefix.as_slice()),
            _ => None,
        })
        .collect();
    history
        .iter()
        .enumerate()
        .filter(|(_, e)| match &e.op {
            Op::Scan { .. } => true,
            Op::Put { key, .. } | Op::Get { key } | Op::Delete { key } => {
                prefixes.iter().any(|p| key.starts_with(p))
            }
        })
        .map(|(i, _)| i)
        .collect()
}

fn subset_unsat_why(history: &[HistoryEntry], indices: &[usize]) -> Option<Vec<String>> {
    if indices.is_empty() || indices.len() > WGL_MAX_OPS {
        return None;
    }
    let sub = LinearizabilityChecker::from_entries(keep_entries(history, indices));
    sub.check_concurrent().err()
}

/// All inclusion-minimal failing subsets of `seed` (seed length must be ≤ `cap`).
fn enumerate_mus_in(
    history: &[HistoryEntry],
    seed: &[usize],
    cap: usize,
) -> Vec<MinimalCounterexample> {
    let n = seed.len();
    if n == 0 || n > cap || n > WGL_MAX_OPS {
        return Vec::new();
    }
    if subset_unsat_why(history, seed).is_none() {
        return Vec::new();
    }

    let mut mus_masks: Vec<u32> = Vec::new();
    let limit = 1u32 << n;
    for size in 1..=n {
        for mask in 1..limit {
            if mask.count_ones() as usize != size {
                continue;
            }
            if mus_masks.iter().copied().any(|mus| (mask & mus) == mus) {
                continue;
            }
            let subset: Vec<usize> = (0..n)
                .filter(|i| (mask & (1u32 << i)) != 0)
                .map(|i| seed[i])
                .collect();
            if subset_unsat_why(history, &subset).is_some() {
                mus_masks.push(mask);
            }
        }
    }

    mus_masks
        .into_iter()
        .map(|mask| {
            let keep: Vec<usize> = (0..n)
                .filter(|i| (mask & (1u32 << i)) != 0)
                .map(|i| seed[i])
                .collect();
            let why = subset_unsat_why(history, &keep).unwrap_or_default();
            MinimalCounterexample {
                original_indices: keep.clone(),
                ops: keep_entries(history, &keep),
                why,
            }
        })
        .collect()
}

fn push_unique_cex(found: &mut Vec<MinimalCounterexample>, cex: MinimalCounterexample) {
    if found
        .iter()
        .any(|e| e.original_indices == cex.original_indices)
    {
        return;
    }
    found.push(cex);
}

fn sort_mus(found: &mut [MinimalCounterexample]) {
    found.sort_by(|a, b| {
        a.ops
            .len()
            .cmp(&b.ops.len())
            .then_with(|| a.original_indices.cmp(&b.original_indices))
    });
}

fn format_op(op: &Op) -> String {
    match op {
        Op::Put { key, value } => format!("PUT key={} val={}", hex_enc(key), hex_enc(value)),
        Op::Delete { key } => format!("DELETE key={}", hex_enc(key)),
        Op::Get { key } => format!("GET key={}", hex_enc(key)),
        Op::Scan { prefix } => format!("SCAN prefix={}", hex_enc(prefix)),
    }
}

fn format_result(r: &OpResult) -> String {
    match r {
        OpResult::Ok => "Ok".into(),
        OpResult::Value(v) => format!("Value({})", option_hex(v.as_deref())),
        OpResult::Scan(items) => format!("Scan({} pairs)", items.len()),
        OpResult::Error(e) => format!("Error({e})"),
    }
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
    fn concurrent_overlapping_put_get_consistent() {
        let mut checker = LinearizabilityChecker::new();
        // Two overlapping puts to different keys, then concurrent gets.
        checker.record_interval(
            0,
            2,
            Some(1),
            Op::Put {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            1,
            3,
            Some(2),
            Op::Put {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            2,
            4,
            Some(1),
            Op::Get { key: b"a".to_vec() },
            OpResult::Value(Some(b"1".to_vec())),
        );
        checker.record_interval(
            2,
            4,
            Some(2),
            Op::Get { key: b"b".to_vec() },
            OpResult::Value(Some(b"2".to_vec())),
        );
        assert!(checker.check_concurrent().is_ok());
        assert!(checker.minimal_counterexample().is_none());
    }

    #[test]
    fn concurrent_stale_read_after_completed_write_is_violation() {
        let mut checker = LinearizabilityChecker::new();
        // Write v2 completes, then write v1 completes, then get sees nothing impossible:
        // Put v1 [0,1], Put v2 [2,3], Get → v1 [4,5] is illegal (must see v2).
        checker.record_interval(
            0,
            1,
            Some(1),
            Op::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            2,
            3,
            Some(2),
            Op::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            4,
            5,
            Some(3),
            Op::Get { key: b"k".to_vec() },
            OpResult::Value(Some(b"v1".to_vec())),
        );
        assert!(checker.check_concurrent().is_err());
    }

    #[test]
    fn minimal_counterexample_shrinks_irrelevant_ops() {
        let mut checker = LinearizabilityChecker::new();
        // Noise: unrelated key.
        checker.record_interval(
            0,
            1,
            Some(9),
            Op::Put {
                key: b"noise".to_vec(),
                value: b"n".to_vec(),
            },
            OpResult::Ok,
        );
        // Core illegal triple on k.
        checker.record_interval(
            2,
            3,
            Some(1),
            Op::Put {
                key: b"k".to_vec(),
                value: b"v1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            4,
            5,
            Some(2),
            Op::Put {
                key: b"k".to_vec(),
                value: b"v2".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            6,
            7,
            Some(3),
            Op::Get { key: b"k".to_vec() },
            OpResult::Value(Some(b"v1".to_vec())),
        );
        // More noise after.
        checker.record_interval(
            8,
            9,
            Some(9),
            Op::Get {
                key: b"noise".to_vec(),
            },
            OpResult::Value(Some(b"n".to_vec())),
        );

        let cex = checker
            .minimal_counterexample()
            .expect("history is non-linearizable");
        assert!(
            cex.ops.len() <= 3,
            "should drop noise ops, got {}:\n{}",
            cex.ops.len(),
            cex.report()
        );
        // Core ops only involve key k.
        for e in &cex.ops {
            match &e.op {
                Op::Put { key, .. } | Op::Get { key } => assert_eq!(key, b"k"),
                other => panic!("unexpected op in cex: {other:?}"),
            }
        }
        let report = cex.report();
        assert!(report.contains("minimal counterexample"));
        assert!(report.contains("PUT") || report.contains("GET"));

        let detailed = checker.check_concurrent_detailed();
        assert!(detailed.is_err());
        let (violations, cex2) = detailed.unwrap_err();
        assert!(!violations.is_empty());
        assert!(cex2.is_some());
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

    /// PUT then GET-miss: each op alone linearizes, the pair does not.
    fn put_then_miss(checker: &mut LinearizabilityChecker, key: &[u8], t0: u64, client: u32) {
        checker.record_interval(
            t0,
            t0 + 1,
            Some(client),
            Op::Put {
                key: key.to_vec(),
                value: b"v".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            t0 + 2,
            t0 + 3,
            Some(client + 1),
            Op::Get { key: key.to_vec() },
            OpResult::Value(None),
        );
    }

    fn mus_keys(cex: &MinimalCounterexample) -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = cex
            .ops
            .iter()
            .filter_map(|e| match &e.op {
                Op::Put { key, .. } | Op::Get { key } | Op::Delete { key } => Some(key.clone()),
                Op::Scan { .. } => None,
            })
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    #[test]
    fn mus_empty_on_linearizable_history() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_next(
            Op::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_next(
            Op::Get { key: b"k".to_vec() },
            OpResult::Value(Some(b"v".to_vec())),
        );
        assert!(checker
            .minimal_unsatisfiable_subsets(WGL_MAX_OPS)
            .is_empty());
        assert!(checker.minimal_counterexample().is_none());
    }

    #[test]
    fn mus_enumerates_independent_multi_key_violations() {
        let mut checker = LinearizabilityChecker::new();
        put_then_miss(&mut checker, b"k1", 0, 1);
        put_then_miss(&mut checker, b"k2", 10, 10);
        // Noise: a write-only key and a miss on an untouched key both linearize alone.
        checker.record_interval(
            20,
            21,
            Some(20),
            Op::Put {
                key: b"ok".to_vec(),
                value: b"x".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            22,
            23,
            Some(21),
            Op::Get {
                key: b"absent".to_vec(),
            },
            OpResult::Value(None),
        );

        assert!(checker.check_concurrent().is_err());
        let muss = checker.minimal_unsatisfiable_subsets(WGL_MAX_OPS);
        assert_eq!(
            muss.len(),
            2,
            "two independent per-key MUSs, got {}:\n{}",
            muss.len(),
            muss.iter()
                .map(|m| m.report())
                .collect::<Vec<_>>()
                .join("\n---\n")
        );
        let mut keys: Vec<Vec<u8>> = muss.iter().flat_map(mus_keys).collect();
        keys.sort();
        assert_eq!(keys, vec![b"k1".to_vec(), b"k2".to_vec()]);
        for mus in &muss {
            assert_eq!(mus.ops.len(), 2, "put-then-miss MUS is the pair");
            let ks = mus_keys(mus);
            assert_eq!(ks.len(), 1, "each MUS stays inside one key");
        }

        let greedy = checker
            .minimal_counterexample()
            .expect("history is non-linearizable");
        assert!(
            muss.iter()
                .any(|m| m.original_indices == greedy.original_indices),
            "greedy shrink should itself be a MUS"
        );
    }

    #[test]
    fn mus_scan_missing_put_is_mixed_partition() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_interval(
            0,
            1,
            Some(1),
            Op::Put {
                key: b"pfx:a".to_vec(),
                value: b"1".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            2,
            3,
            Some(2),
            Op::Scan {
                prefix: b"pfx:".to_vec(),
            },
            OpResult::Scan(vec![]),
        );

        assert!(checker.check_sequential().is_err());
        let muss = checker.minimal_unsatisfiable_subsets(WGL_MAX_OPS);
        assert_eq!(
            muss.len(),
            1,
            "one mixed put+scan MUS, got {}:\n{}",
            muss.len(),
            muss.iter()
                .map(|m| m.report())
                .collect::<Vec<_>>()
                .join("\n---\n")
        );
        assert_eq!(muss[0].ops.len(), 2);
        assert!(matches!(muss[0].ops[0].op, Op::Put { .. }));
        assert!(matches!(muss[0].ops[1].op, Op::Scan { .. }));
        assert_eq!(muss[0].original_indices, vec![0, 1]);
    }

    #[test]
    fn mus_scan_fabricated_pair_is_scan_only() {
        let mut checker = LinearizabilityChecker::new();
        checker.record_interval(
            0,
            1,
            Some(1),
            Op::Put {
                key: b"other".to_vec(),
                value: b"x".to_vec(),
            },
            OpResult::Ok,
        );
        checker.record_interval(
            2,
            3,
            Some(2),
            Op::Scan {
                prefix: b"pfx:".to_vec(),
            },
            OpResult::Scan(vec![(b"pfx:ghost".to_vec(), b"1".to_vec())]),
        );

        let muss = checker.minimal_unsatisfiable_subsets(WGL_MAX_OPS);
        assert_eq!(muss.len(), 1, "fabricated scan is a size-1 MUS");
        assert_eq!(muss[0].ops.len(), 1);
        assert!(matches!(muss[0].ops[0].op, Op::Scan { .. }));
        assert_eq!(muss[0].original_indices, vec![1]);
    }

    #[test]
    fn mus_cap_skips_oversize_universe() {
        let mut checker = LinearizabilityChecker::new();
        put_then_miss(&mut checker, b"k", 0, 1);
        // cap=1 cannot brute-force a 2-op universe, so no MUS is enumerated.
        assert!(checker.minimal_unsatisfiable_subsets(1).is_empty());
        assert_eq!(checker.minimal_unsatisfiable_subsets(2).len(), 1);
    }
}
