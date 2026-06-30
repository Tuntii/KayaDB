//! Optional Linux eBPF observability for KayaDB.
//!
//! **Default path (all platforms):** userspace tap fed by engine WAL fsync stats or
//! explicit `report_fsync` calls — no kernel privileges required.
//!
//! **Linux + `kernel-probes` feature:** attempts to compile `bpf/fsync_latency.bpf.c`
//! (clang + vendored headers, optional bpftool vmlinux) and attach kprobes via aya.
//! When kernel attach succeeds, per-op ring-buffer samples replace synthetic tap
//! injection in `sync_from_engine_stats`. Falls back to userspace tap when BPF build
//! or attach fails.

pub mod backend;
mod event;
mod histogram;
mod manager;
mod trace;

pub use backend::{EventBackend, SimulatedBackend, TapBackend};
pub use backend::kernel::{parse_raw_fsync_event, parse_ringbuf_batch, RawFsyncEvent};
pub use event::{ProbeEvent, SyscallKind};
pub use histogram::{FsyncHistogram, FSYNC_LATENCY_BUCKETS_US};
pub use manager::{shared_probe_manager, ProbeConfig, ProbeManager, ProbeStatus, SharedProbeManager};
pub use trace::{
    filter_wal_events, replay_validate, seeded_fsync_events, write_trace, TraceHeader,
    TraceReplayError,
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

    /// Documented build prerequisites for kernel probes.
    pub const BUILD_NOTES: &str = concat!(
        "Userspace tap is default. Kernel kprobes need linux + --features kernel-probes + clang/llvm; ",
        "optional bpftool for accurate vmlinux.h; CAP_BPF for live attach. ",
        "See bpf/include/ bundled headers and bpf/fsync_latency.bpf.c."
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