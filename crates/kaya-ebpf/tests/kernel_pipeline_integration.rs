//! Cross-platform kernel-slot integration: attach → stream → histogram → trace.

use kaya_ebpf::{ProbeConfig, ProbeManager};
use tempfile::tempdir;

#[test]
fn kernel_backend_streams_through_pipeline() {
    let dir = tempdir().unwrap();
    let seed = 4242;
    let mut mgr = ProbeManager::new(ProbeConfig::for_kernel_slot(
        dir.path(),
        seed,
        "kernel-pipeline-integration",
    ));

    mgr.attach().expect("kernel-simulated attach");
    assert!(mgr.kernel_streaming(), "kernel slot must be streaming after attach");
    assert!(
        mgr.backend_name().contains("kernel"),
        "backend must be kernel-family, got {}",
        mgr.backend_name()
    );

    mgr.pump_events();
    assert!(
        mgr.histogram().has_nonzero_observations(),
        "kernel slot must produce non-zero histogram after first drain"
    );

    mgr.sync_from_engine_stats(2_000, 350);
    assert!(
        mgr.histogram().total_count() >= 2,
        "WAL activity must append kernel-shaped events"
    );

    let events = mgr.events();
    assert!(
        events.iter().any(|e| matches!(e, kaya_ebpf::ProbeEvent::FsyncLatency { ts_ns, .. } if *ts_ns > 0)),
        "kernel-shaped events must carry non-zero ts_ns"
    );

    mgr.write_status().unwrap();
    mgr.flush_trace().unwrap();
    let replayed = mgr.validate_trace().expect("trace replay");
    assert!(!replayed.is_empty());

    let status_raw = std::fs::read_to_string(dir.path().join("ebpf/status.json")).unwrap();
    assert!(status_raw.contains("kernel"), "status.json must record kernel backend");

    mgr.detach();
    assert!(!mgr.is_attached());
}