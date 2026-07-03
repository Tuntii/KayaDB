//! Optional userspace probe markers (USDT-shaped hooks). No-op when no sink is installed.

use std::sync::{Mutex, OnceLock};

/// Durability boundary where a marker may fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbeMarkerSite {
    WalFsync,
    Flush,
}

impl ProbeMarkerSite {
    /// Stable site label shared by USDT markers, trace.jsonl, and OTel span names.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WalFsync => "wal_fsync",
            Self::Flush => "flush",
        }
    }
}

/// Enter/exit phase for a boundary marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProbeMarkerPhase {
    Enter,
    Exit,
}

type MarkerCallback = Box<dyn Fn(ProbeMarkerSite, ProbeMarkerPhase, Option<u64>) + Send + Sync>;

fn marker_slot() -> &'static Mutex<Option<MarkerCallback>> {
    static SLOT: OnceLock<Mutex<Option<MarkerCallback>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

fn span_slot() -> &'static Mutex<Option<MarkerCallback>> {
    static SLOT: OnceLock<Mutex<Option<MarkerCallback>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

/// Install or clear the global marker sink (typically from `kaya-ebpf` when `--ebpf` is on).
pub fn set_probe_marker_callback(callback: Option<MarkerCallback>) {
    *marker_slot().lock().expect("probe marker mutex poisoned") = callback;
}

/// Install or clear a parallel span sink (typically OpenTelemetry when `otel` feature is on).
pub fn set_probe_span_callback(callback: Option<MarkerCallback>) {
    *span_slot().lock().expect("probe span mutex poisoned") = callback;
}

/// Emit a marker; no-op when ebpf is off or no sink is registered.
pub fn emit_probe_marker(site: ProbeMarkerSite, phase: ProbeMarkerPhase, duration_us: Option<u64>) {
    if let Some(cb) = marker_slot()
        .lock()
        .expect("probe marker mutex poisoned")
        .as_ref()
    {
        cb(site, phase, duration_us);
    }
    if let Some(cb) = span_slot()
        .lock()
        .expect("probe span mutex poisoned")
        .as_ref()
    {
        cb(site, phase, duration_us);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn emit_is_noop_without_sink() {
        set_probe_marker_callback(None);
        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Enter, None);
    }

    #[test]
    fn sink_receives_enter_and_exit() {
        let count = std::sync::Arc::new(AtomicU64::new(0));
        let count_cb = count.clone();
        set_probe_marker_callback(Some(Box::new(move |site, phase, dur| {
            count_cb.fetch_add(1, Ordering::SeqCst);
            assert_eq!(site, ProbeMarkerSite::Flush);
            match phase {
                ProbeMarkerPhase::Enter => assert!(dur.is_none()),
                ProbeMarkerPhase::Exit => assert_eq!(dur, Some(42)),
            }
        })));
        emit_probe_marker(ProbeMarkerSite::Flush, ProbeMarkerPhase::Enter, None);
        emit_probe_marker(ProbeMarkerSite::Flush, ProbeMarkerPhase::Exit, Some(42));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        set_probe_marker_callback(None);
    }
}
