//! Bounded chaos-style workload producing a capturable eBPF trace artifact.

use kaya_ebpf::{ProbeConfig, ProbeManager};
use tempfile::tempdir;

#[test]
fn bounded_chaos_workload_produces_trace_with_durability_events() {
    let dir = tempdir().unwrap();
    let seed = 2026;
    let mut mgr = ProbeManager::new(ProbeConfig::for_kernel_slot(dir.path(), seed, "chaos-bounded"));
    mgr.attach().unwrap();
    mgr.pump_events();

    for step in 1..=5u64 {
        mgr.sync_from_engine_stats(step * 200, 80 + step * 20);
    }

    mgr.write_status().unwrap();
    mgr.flush_trace().unwrap();

    let replayed = mgr.validate_trace().unwrap();
    assert!(replayed.len() >= 5, "expected durability events in trace");
    assert!(dir.path().join("ebpf/trace.jsonl").exists());
    assert!(dir.path().join("ebpf/status.json").exists());
    assert!(mgr.backend_name().contains("kernel"));

    mgr.detach();
}