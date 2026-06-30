use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;

use crate::backend::{EventBackend, StubBackend};
use crate::event::{ProbeEvent, SyscallKind};
use crate::histogram::FsyncHistogram;
use crate::trace::{write_trace, TraceReplayError};

/// Configuration for the in-process probe runtime.
#[derive(Debug, Clone)]
pub struct ProbeConfig {
    pub seed: u64,
    pub config_hash: String,
    pub trace_path: PathBuf,
    pub status_path: PathBuf,
    /// When true, also stream seeded simulated events (CI / non-CAP_BPF fallback).
    pub simulated_fallback: bool,
}

impl ProbeConfig {
    pub fn for_data_dir(data_dir: impl AsRef<Path>, seed: u64, config_hash: impl Into<String>) -> Self {
        let data_dir = data_dir.as_ref();
        Self {
            seed,
            config_hash: config_hash.into(),
            trace_path: data_dir.join("ebpf/trace.jsonl"),
            status_path: data_dir.join("ebpf/status.json"),
            simulated_fallback: true,
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

enum BackendKind {
    #[cfg(target_os = "linux")]
    Linux(LinuxCompositeBackend),
    #[cfg(not(target_os = "linux"))]
    Stub(StubBackend),
}

pub struct ProbeManager {
    config: ProbeConfig,
    backend: BackendKind,
    histogram: FsyncHistogram,
    collected: Vec<ProbeEvent>,
    last_wal_fsync_total_us: u64,
    last_wal_fsync_max_us: u64,
}

impl ProbeManager {
    pub fn new(config: ProbeConfig) -> Self {
        let seed = if config.simulated_fallback {
            Some(config.seed)
        } else {
            None
        };
        let backend = {
            #[cfg(target_os = "linux")]
            {
                BackendKind::Linux(LinuxCompositeBackend::new(seed))
            }
            #[cfg(not(target_os = "linux"))]
            {
                BackendKind::Stub(StubBackend::new(seed))
            }
        };
        Self {
            config,
            backend,
            histogram: FsyncHistogram::new(),
            collected: Vec::new(),
            last_wal_fsync_total_us: 0,
            last_wal_fsync_max_us: 0,
        }
    }

    pub fn attach(&mut self) -> Result<(), String> {
        match &mut self.backend {
            #[cfg(target_os = "linux")]
            BackendKind::Linux(b) => b.attach(),
            #[cfg(not(target_os = "linux"))]
            BackendKind::Stub(b) => b.attach(),
        }
    }

    pub fn detach(&mut self) -> bool {
        self.pump_events();
        match &mut self.backend {
            #[cfg(target_os = "linux")]
            BackendKind::Linux(b) => b.detach(),
            #[cfg(not(target_os = "linux"))]
            BackendKind::Stub(b) => b.detach(),
        }
    }

    pub fn is_attached(&self) -> bool {
        match &self.backend {
            #[cfg(target_os = "linux")]
            BackendKind::Linux(b) => b.is_attached(),
            #[cfg(not(target_os = "linux"))]
            BackendKind::Stub(b) => b.is_attached(),
        }
    }

    pub fn streaming(&self) -> bool {
        self.is_attached()
    }

    pub fn histogram(&self) -> &FsyncHistogram {
        &self.histogram
    }

    pub fn events(&self) -> &[ProbeEvent] {
        &self.collected
    }

    pub fn status(&self) -> ProbeStatus {
        ProbeStatus {
            attached: self.is_attached(),
            streaming: self.streaming(),
            backend: self.backend_name().to_owned(),
            events_collected: self.collected.len() as u64,
            seed: self.config.seed,
            trace_path: self.config.trace_path.display().to_string(),
        }
    }

    pub fn pump_events(&mut self) {
        let drained = match &mut self.backend {
            #[cfg(target_os = "linux")]
            BackendKind::Linux(b) => b.drain_events(),
            #[cfg(not(target_os = "linux"))]
            BackendKind::Stub(b) => b.drain_events(),
        };
        for mut event in drained {
            let seq = self.collected.len() as u64 + 1;
            let ProbeEvent::FsyncLatency { seq: ref mut event_seq, .. } = &mut event;
            *event_seq = seq;
            self.histogram.ingest(&event);
            self.collected.push(event);
        }
    }

    pub fn sync_from_engine_stats(&mut self, wal_fsync_total_us: u64, wal_fsync_max_us: u64) {
        if !self.is_attached() {
            return;
        }
        let delta = wal_fsync_total_us.saturating_sub(self.last_wal_fsync_total_us);
        if delta > 0 {
            let ts_ns = now_ns();
            self.tap_report(SyscallKind::Fsync, wal_fsync_max_us.max(1), ts_ns);
            if delta > wal_fsync_max_us {
                self.tap_report(
                    SyscallKind::Fdatasync,
                    (delta - wal_fsync_max_us).max(1),
                    ts_ns.wrapping_add(1),
                );
            }
        }
        self.last_wal_fsync_total_us = wal_fsync_total_us;
        self.last_wal_fsync_max_us = wal_fsync_max_us;
        self.pump_events();
    }

    pub fn report_fsync(&mut self, syscall: SyscallKind, latency_us: u64) {
        self.tap_report(syscall, latency_us, now_ns());
        self.pump_events();
    }

    fn tap_report(&mut self, syscall: SyscallKind, latency_us: u64, ts_ns: u64) {
        #[cfg(target_os = "linux")]
        if let BackendKind::Linux(b) = &mut self.backend {
            b.report_fsync(syscall, latency_us, ts_ns);
        }
        let _ = (syscall, latency_us, ts_ns);
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
            &self.collected,
        )
    }

    pub fn validate_trace(&self) -> Result<Vec<ProbeEvent>, TraceReplayError> {
        crate::trace::replay_validate(&self.config.trace_path, self.config.seed)
    }

    fn backend_name(&self) -> &'static str {
        match &self.backend {
            #[cfg(target_os = "linux")]
            BackendKind::Linux(b) => b.backend_name(),
            #[cfg(not(target_os = "linux"))]
            BackendKind::Stub(b) => b.backend_name(),
        }
    }
}

/// Thread-safe handle shared by the server metrics loop.
pub type SharedProbeManager = Arc<Mutex<ProbeManager>>;

pub fn shared_probe_manager(config: ProbeConfig) -> SharedProbeManager {
    Arc::new(Mutex::new(ProbeManager::new(config)))
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn attach_detach_lifecycle_streams_simulated_events() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_data_dir(dir.path(), 7, "cfg"));
        assert!(!mgr.is_attached());
        mgr.attach().unwrap();
        assert!(mgr.is_attached());
        mgr.pump_events();
        assert!(!mgr.events().is_empty());
        assert!(mgr.histogram().total_count() > 0);
        mgr.detach();
        assert!(!mgr.is_attached());
    }

    #[test]
    fn engine_stats_sync_emits_tap_events_on_linux_or_stub() {
        let dir = tempdir().unwrap();
        let mut mgr = ProbeManager::new(ProbeConfig::for_data_dir(dir.path(), 3, "cfg"));
        mgr.attach().unwrap();
        let before = mgr.events().len();
        mgr.sync_from_engine_stats(0, 0);
        let after_zero = mgr.events().len();
        mgr.sync_from_engine_stats(500, 120);
        mgr.pump_events();
        assert!(mgr.events().len() >= after_zero);
        let _ = before;
        #[cfg(target_os = "linux")]
        assert!(mgr.events().len() > after_zero);
    }
}