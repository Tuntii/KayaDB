//! Optional Linux eBPF observability for KayaDB.
//!
//! **Server `--ebpf` path:** explicit kernel slot (`KernelLive` when attach succeeds on
//! Linux + `kernel-probes`, else `KernelSimulated`). Userspace tap is **not** mixed into
//! `kaya_ebpf_*` metrics/traces. Engine counters remain `kaya_wal_fsync_*`.

pub mod backend;
mod event;
mod histogram;
mod manager;
mod markers;
mod pipeline;
mod trace;

pub use backend::kernel::{parse_raw_fsync_event, parse_ringbuf_batch, RawFsyncEvent};
pub use backend::{
    BackendSelection, EventBackend, KernelSimulatedBackend, ProbeBackend, SimulatedBackend,
    TapBackend,
};
pub use event::{MarkerPhase, MarkerSite, ProbeEvent, PublishSyscallKind, SyscallKind};
pub use histogram::{FsyncHistogram, FSYNC_LATENCY_BUCKETS_US};
pub use manager::{
    shared_probe_manager, ProbeConfig, ProbeManager, ProbeStatus, SharedProbeManager,
};
pub use markers::{clear_usdt_marker_sink, install_usdt_marker_sink};
pub use pipeline::EventPipeline;
pub use trace::{
    filter_publish_events, filter_wal_events, replay_validate, seeded_fsync_events,
    seeded_mixed_durability_events, write_trace, TraceHeader, TraceReplayError,
};

/// Metadata for a bpftrace probe script shipped with KayaDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeInfo {
    pub name: &'static str,
    pub script_path: &'static str,
}

/// Whether an in-process probe is attached (checks the optional global handle).
pub fn probe_attached(manager: Option<&ProbeManager>) -> bool {
    manager.is_some_and(|m| m.is_attached())
}

#[cfg(target_os = "linux")]
pub mod linux {
    //! Linux-only catalog of bpftrace scripts under `scripts/ebpf/`.

    const PROBES: &[(&str, &str)] = &[
        ("fsync-latency", "scripts/ebpf/fsync-latency.bt"),
        ("block-io-latency", "scripts/ebpf/block-io-latency.bt"),
        ("syscall-timeline", "scripts/ebpf/syscall-timeline.bt"),
        ("durability-syscalls", "scripts/ebpf/durability-syscalls.bt"),
    ];

    pub const BUILD_NOTES: &str = concat!(
        "Server --ebpf uses kernel slot (live or simulated). ",
        "KernelLive needs linux + --features kernel-probes + clang + CAP_BPF. ",
        "See bpf/include/ and bpf/fsync_latency.bpf.c."
    );

    pub fn available_scripts() -> Vec<&'static str> {
        PROBES.iter().map(|(_, path)| *path).collect()
    }

    pub fn probe_catalog() -> Vec<super::ProbeInfo> {
        PROBES
            .iter()
            .map(|(name, path)| super::ProbeInfo {
                name,
                script_path: path,
            })
            .collect()
    }
}

#[cfg(not(target_os = "linux"))]
mod stub {
    pub fn available_scripts() -> Vec<&'static str> {
        Vec::new()
    }

    pub fn probe_catalog() -> Vec<super::ProbeInfo> {
        Vec::new()
    }
}

pub fn available_scripts() -> Vec<&'static str> {
    inner_available_scripts()
}

pub fn probe_catalog() -> Vec<ProbeInfo> {
    inner_probe_catalog()
}

#[cfg(target_os = "linux")]
fn inner_available_scripts() -> Vec<&'static str> {
    linux::available_scripts()
}

#[cfg(not(target_os = "linux"))]
fn inner_available_scripts() -> Vec<&'static str> {
    stub::available_scripts()
}

#[cfg(target_os = "linux")]
fn inner_probe_catalog() -> Vec<ProbeInfo> {
    linux::probe_catalog()
}

#[cfg(not(target_os = "linux"))]
fn inner_probe_catalog() -> Vec<ProbeInfo> {
    stub::probe_catalog()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn available_scripts_and_catalog_agree() {
        let scripts = available_scripts();
        let catalog = probe_catalog();
        let paths: Vec<_> = catalog.iter().map(|p| p.script_path).collect();
        assert_eq!(scripts, paths);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_catalog_lists_known_probes() {
        let catalog = probe_catalog();
        assert_eq!(catalog.len(), 4);
        assert!(catalog.iter().any(|p| p.name == "fsync-latency"));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_returns_empty_catalog() {
        assert!(available_scripts().is_empty());
        assert!(probe_catalog().is_empty());
    }

    #[test]
    fn probe_manager_lifecycle_and_trace_roundtrip() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_tests(dir.path(), 42, "unit-test"));
        mgr.attach().unwrap();
        mgr.pump_events();
        mgr.flush_trace().unwrap();
        let replayed = mgr.validate_trace().unwrap();
        assert!(!replayed.is_empty());
        mgr.detach();
        assert!(!mgr.is_attached());
    }
}
