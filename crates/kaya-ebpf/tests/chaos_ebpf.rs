//! Bounded chaos-style workload producing a capturable eBPF trace artifact.

use kaya_ebpf::{ProbeConfig, ProbeManager, SyscallKind};
use tempfile::tempdir;

#[test]
fn bounded_chaos_workload_produces_trace_with_durability_events() {
    let dir = tempdir().unwrap();
    let seed = 2026;
    let mut mgr = ProbeManager::new(ProbeConfig::for_data_dir(dir.path(), seed, "chaos-bounded"));
    // report_fsync drives real tap path (not simulated prefill).
    mgr.attach().unwrap();

    // Simulate workload + crash-injection window via userspace tap (report_fsync).
    for i in 0..5u64 {
        mgr.report_fsync(SyscallKind::Fsync, 80 + i * 20);
        mgr.report_fsync(SyscallKind::Fdatasync, 40 + i * 10);
    }
    mgr.pump_events();
    mgr.write_status().unwrap();
    mgr.flush_trace().unwrap();

    let replayed = mgr.validate_trace().unwrap();
    assert!(replayed.len() >= 5, "expected durability events in trace");
    assert!(dir.path().join("ebpf/trace.jsonl").exists());
    assert!(dir.path().join("ebpf/status.json").exists());

    mgr.detach();
}