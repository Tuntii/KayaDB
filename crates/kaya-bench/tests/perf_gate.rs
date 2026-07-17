//! Performance regression gate for KayaDB smoke workloads (M13 + M25 v2).
//!
//! This provides the CI "performance envelope" regression gate.
//! It is intentionally a *coarse* gate: it will only fail on severe regressions
//! (e.g. allocator changes, accidental quadratic behavior, lock contention blowups).
//!
//! Run in CI with:
//!   cargo test -p kaya-bench --test perf_gate --release
//!
//! Budgets use release profile + SimDisk (deterministic, CPU-bound).
//! They tolerate GitHub Actions runner variance.

use std::time::{Duration, Instant};

use kaya_bench::{run_smoke_multi_range_2pc, run_smoke_put_get, run_smoke_txn_multi_key};
use tokio::runtime::Builder;

// Generous even for debug profile + GH runner variance.
// In release the observed put/get is ~18-20µs. We catch catastrophic (>>10x) regressions.
fn smoke_budget() -> Duration {
    // Tight in release (the profile used by `cargo bench` and our CI gate).
    // Loose in debug so that normal `cargo test --workspace` never flakes on this.
    if cfg!(debug_assertions) {
        Duration::from_millis(10)
    } else {
        // ~25x headroom over the ~18-20µs we see in bench profile.
        // Still fails on 10-50x+ regressions.
        Duration::from_micros(500)
    }
}

/// Loose budget for multi-key SI commit (8 puts + verify).
/// Release path is typically sub-millisecond on SimDisk; keep large headroom for CI.
fn txn_multi_key_budget() -> Duration {
    if cfg!(debug_assertions) {
        Duration::from_millis(50)
    } else {
        Duration::from_millis(5)
    }
}

/// Loose budget for multi-range 2PC prepare+commit materialization (4 keys).
/// More WAL/system-key writes than SI multi-key; still fails only on severe regressions.
fn multi_range_2pc_budget() -> Duration {
    if cfg!(debug_assertions) {
        Duration::from_millis(100)
    } else {
        Duration::from_millis(10)
    }
}

const ITERS: usize = 3;

/// CI regression gate. Fails the build on gross performance regression of the
/// primary smoke path (relaxed put+get using engine hot path + SimDisk).
#[test]
fn perf_smoke_put_get_under_budget() {
    let rt = Builder::new_current_thread().build().unwrap();

    let budget = smoke_budget();
    for i in 0..ITERS {
        let start = Instant::now();
        rt.block_on(run_smoke_put_get());
        let elapsed = start.elapsed();

        assert!(
            elapsed <= budget,
            "PERF REGRESSION GATE FAILED (iter {}): smoke_put_get took {:?}, budget was {:?}. \
             This indicates a significant slowdown in the core engine path. \
             Investigate recent changes to engine, wal, lsm, or alloc behavior.",
            i,
            elapsed,
            budget
        );
    }
}

/// M25 perf envelope v2: multi-key SI transaction smoke under loose budget.
#[test]
fn perf_smoke_txn_multi_key_under_budget() {
    let rt = Builder::new_current_thread().build().unwrap();

    let budget = txn_multi_key_budget();
    for i in 0..ITERS {
        let start = Instant::now();
        rt.block_on(run_smoke_txn_multi_key());
        let elapsed = start.elapsed();

        assert!(
            elapsed <= budget,
            "PERF REGRESSION GATE FAILED (iter {}): smoke_txn_multi_key took {:?}, budget was {:?}. \
             Investigate SI intent tables, txn_commit, or apply_mutations regressions.",
            i,
            elapsed,
            budget
        );
    }
}

/// M25 perf envelope v2: multi-range 2PC participant path under loose budget.
#[test]
fn perf_smoke_multi_range_2pc_under_budget() {
    let rt = Builder::new_current_thread().build().unwrap();

    let budget = multi_range_2pc_budget();
    for i in 0..ITERS {
        let start = Instant::now();
        rt.block_on(run_smoke_multi_range_2pc());
        let elapsed = start.elapsed();

        assert!(
            elapsed <= budget,
            "PERF REGRESSION GATE FAILED (iter {}): smoke_multi_range_2pc took {:?}, budget was {:?}. \
             Investigate apply_txn_prepare / apply_txn_commit_2pc / system-key WAL path.",
            i,
            elapsed,
            budget
        );
    }
}
