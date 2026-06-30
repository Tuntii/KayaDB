use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

use crate::backend::{BackendSelection, KernelSimulatedBackend, ProbeBackend};
use crate::event::{ProbeEvent, SyscallKind};
use crate::pipeline::EventPipeline;
use crate::trace::{write_trace, TraceReplayError};

/// Configuration for the in-process probe runtime.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub seed: u64,
    pub config_hash: String,
    pub trace_path: PathBuf,
    pub status_path: PathBuf,
    pub backend_selection: BackendSelection,
}

impl ProbeConfig {
    /// Server `--ebpf`: kernel slot (live when available, else kernel-simulated).
    pub fn for_server(data_dir: impl AsRef<Path>, seed: u64, config_hash: impl Into<String>) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            seed,
            config_hash: config_hash.into(),
            trace_path: data_dir.join("ebpf/trace.jsonl"),
            status_path: data_dir.join("ebpf/status.json"),
            backend_selection: BackendSelection::KernelPreferred,
        }
    }

    /// Explicit kernel-simulated slot (integration tests, Windows harness).
    pub fn for_kernel_slot(data_dir: impl AsRef<Path>, seed: u64, config_hash: impl Into<String>) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            seed,
            config_hash: config_hash.into(),
            trace_path: data_dir.join("ebpf/trace.jsonl"),
            status_path: data_dir.join("ebpf/status.json"),
            backend_selection: BackendSelection::KernelSimulated,
        }
    }

    /// Legacy name — routes to server kernel slot.
    pub fn for_data_dir(data_dir: impl AsRef<Path>, seed: u64, config_hash: impl Into<String>) -> Self {
        Self::for_server(data_dir, seed, config_hash)
    }

    /// Seeded test-simulated backend (non-kernel family).
    pub fn for_tests(data_dir: impl AsRef<Path>, seed: u64, config_hash: impl Into<String>) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            seed,
            config_hash: config_hash.into(),
            trace_path: data_dir.join("ebpf/trace.jsonl"),
            status_path: data_dir.join("ebpf/status.json"),
            backend_selection: BackendSelection::TestSimulated,
        }
    }

    /// Userspace tap backend (unit tests only).
    pub fn for_tap(data_dir: impl AsRef<Path>, seed: u64, config_hash: impl Into<String>) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            seed,
            config_hash: config_hash.into(),
            trace_path: data_dir.join("ebpf/trace.jsonl"),
            status_path: data_dir.join("ebpf/status.json"),
            backend_selection: BackendSelection::Tap,
        }
    }
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ProbeStatus {
    pub attached: bool,
    pub streaming: bool,
    pub backend: String,
    pub events_collected: u64,
    pub seed: u64,
    pub trace_path: String,
}

pub struct ProbeManager {
    config: ProbeConfig,
    backend: ProbeBackend,
    pipeline: EventPipeline,
    last_wal_fsync_total_us: u64,
    last_wal_fsync_max_us: u64,
}

impl ProbeManager {
    pub fn new(config: ProbeConfig) -> Self {
        let backend = ProbeBackend::build(config.backend_selection, config.seed);
        Self {
            config,
            backend,
            pipeline: EventPipeline::new(),
            last_wal_fsync_total_us: 0,
            last_wal_fsync_max_us: 0,
        }
    }

    pub fn attach(&mut self) -> Result<(), String> {
        match self.backend.attach() {
            Ok(()) => Ok(()),
            Err(_)
                if self.config.backend_selection == BackendSelection::KernelPreferred =>
            {
                self.backend =
                    ProbeBackend::KernelSimulated(KernelSimulatedBackend::new(self.config.seed));
                self.backend.attach()
            }
            Err(e) => Err(e),
        }
    }

    pub fn detach(&mut self) -> bool {
        self.pump_events();
        self.backend.detach()
    }

    pub fn is_attached(&self) -> bool {
        self.backend.is_attached()
    }

    pub fn streaming(&self) -> bool {
        self.is_attached()
    }

    pub fn histogram(&self) -> &crate::histogram::FsyncHistogram {
        self.pipeline.histogram()
    }

    pub fn events(&self) -> &[ProbeEvent] {
        self.pipeline.events()
    }

    pub fn status(&self) -> ProbeStatus {
        ProbeStatus {
            attached: self.is_attached(),
            streaming: self.streaming(),
            backend: self.backend_name().to_owned(),
            events_collected: self.pipeline.event_count(),
            seed: self.config.seed,
            trace_path: self.config.trace_path.display().to_string(),
        }
    }

    pub fn pump_events(&mut self) {
        let drained = self.backend.drain_events();
        self.pipeline.ingest_batch(drained);
    }

    /// Sync WAL activity into the active backend slot, then drain.
    pub fn sync_from_engine_stats(&mut self, wal_fsync_total_us: u64, wal_fsync_max_us: u64) {
        if !self.is_attached() {
            return;
        }
        let delta = wal_fsync_total_us.saturating_sub(self.last_wal_fsync_total_us);
        self.backend.sync_wal_activity(delta, wal_fsync_max_us);
        self.last_wal_fsync_total_us = wal_fsync_total_us;
        self.last_wal_fsync_max_us = wal_fsync_max_us;
        self.pump_events();
    }

    pub fn report_fsync(&mut self, syscall: SyscallKind, latency_us: u64) {
        self.backend.report_fsync(syscall, latency_us, 0);
        self.pump_events();
    }

    pub fn write_status(&self) -> std::io::Result<()> {
        if let Some(parent) = self.config.status_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(&self.status())?;
        std::fs::write(&self.config.status_path, json)
    }

    pub fn flush_trace(&self) -> std::io::Result<()> {
        write_trace(
            &self.config.trace_path,
            self.config.seed,
            &self.config.config_hash,
            self.pipeline.events(),
        )
    }

    pub fn validate_trace(&self) -> Result<Vec<ProbeEvent>, TraceReplayError> {
        crate::trace::replay_validate(&self.config.trace_path, self.config.seed)
    }

    pub fn backend_name(&self) -> &'static str {
        self.backend.backend_name()
    }

    pub fn kernel_streaming(&self) -> bool {
        self.backend.kernel_streaming()
    }

    pub fn is_kernel_family_backend(&self) -> bool {
        self.backend.is_kernel_family()
    }
}

/// Thread-safe handle shared by the server probe pump and metrics loop.
pub type SharedProbeManager = Arc<Mutex<ProbeManager>>;

pub fn shared_probe_manager(config: ProbeConfig) -> SharedProbeManager {
    Arc::new(Mutex::new(ProbeManager::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn kernel_slot_config_streams_kernel_family_backend() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_kernel_slot(dir.path(), 42, "kernel-slot"));
        mgr.attach().unwrap();
        mgr.pump_events();
        assert!(mgr.is_kernel_family_backend());
        assert!(mgr.kernel_streaming());
        assert!(mgr.backend_name().contains("kernel"));
        assert!(mgr.histogram().total_count() > 0);
        mgr.flush_trace().unwrap();
        assert!(dir.path().join("ebpf/trace.jsonl").exists());
        mgr.detach();
    }

    #[test]
    fn server_config_uses_kernel_family_not_tap() {
        let dir = tempdir().unwrap();
        let mgr = ProbeManager::new(ProbeConfig::for_server(dir.path(), 1, "srv"));
        assert!(mgr.is_kernel_family_backend());
        assert!(!mgr.backend_name().contains("tap"));
    }

    #[test]
    fn test_config_streams_simulated_events() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_tests(dir.path(), 7, "cfg"));
        mgr.attach().unwrap();
        mgr.pump_events();
        assert!(!mgr.is_kernel_family_backend());
        assert!(!mgr.events().is_empty());
        assert!(mgr.histogram().total_count() > 0);
        mgr.detach();
    }

    #[test]
    fn tap_config_accepts_report_fsync() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_tap(dir.path(), 3, "tap"));
        mgr.attach().unwrap();
        mgr.report_fsync(SyscallKind::Fsync, 120);
        assert_eq!(mgr.histogram().total_count(), 1);
        assert_eq!(mgr.backend_name(), "userspace-tap");
    }

    #[test]
    fn kernel_slot_sync_from_engine_stats_emits_events() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_kernel_slot(dir.path(), 3, "cfg"));
        mgr.attach().unwrap();
        let boot_count = mgr.histogram().total_count();
        mgr.sync_from_engine_stats(500, 120);
        assert!(mgr.histogram().total_count() > boot_count);
        assert!(mgr.kernel_streaming());
    }

    #[test]
    fn kernel_slot_sync_does_not_duplicate_on_zero_delta() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_kernel_slot(dir.path(), 3, "cfg"));
        mgr.attach().unwrap();
        mgr.pump_events();
        let after_boot = mgr.histogram().total_count();
        mgr.sync_from_engine_stats(500, 120);
        let after_first = mgr.histogram().total_count();
        assert!(after_first > after_boot);
        mgr.sync_from_engine_stats(500, 120);
        assert_eq!(mgr.histogram().total_count(), after_first);
    }
}