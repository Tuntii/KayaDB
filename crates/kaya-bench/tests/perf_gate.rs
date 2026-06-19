//! Performance regression gate for KayaDB smoke workload.
//!
//! This provides the CI "performance envelope" regression gate (M13-6).
//! It is intentionally a *coarse* gate: it will only fail on severe regressions
//! (e.g. allocator changes, accidental quadratic behavior, lock contention blowups).
//!
//! Run in CI with:
//!   cargo test -p kaya-bench --test perf_gate --release
//!
//! Budgets use release profile + SimDisk (deterministic, CPU-bound).
//! They tolerate GitHub Actions runner variance.

use std::time::{Duration, Instant};

use kaya_bench::run_smoke_put_get;
use tokio::runtime::Builder;

// Generous even for debug profile + GH runner variance.
// In release the observed is ~18-20µs. We catch catastrophic (>>10x) regressions.
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