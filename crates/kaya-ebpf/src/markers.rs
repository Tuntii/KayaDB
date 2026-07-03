use kaya_core::{ProbeMarkerPhase, ProbeMarkerSite};

use crate::event::{MarkerPhase, MarkerSite};
use crate::manager::SharedProbeManager;

fn map_site(site: ProbeMarkerSite) -> MarkerSite {
    match site {
        ProbeMarkerSite::WalFsync => MarkerSite::WalFsync,
        ProbeMarkerSite::Flush => MarkerSite::Flush,
    }
}

fn map_phase(phase: ProbeMarkerPhase) -> MarkerPhase {
    match phase {
        ProbeMarkerPhase::Enter => MarkerPhase::Enter,
        ProbeMarkerPhase::Exit => MarkerPhase::Exit,
    }
}

/// Install a global marker sink that forwards WAL/flush boundaries into `mgr`.
pub fn install_usdt_marker_sink(mgr: SharedProbeManager) {
    let sink = mgr;
    kaya_core::set_probe_marker_callback(Some(Box::new(move |site, phase, duration_us| {
        let mut guard = sink.lock();
        guard.record_usdt_marker(map_site(site), map_phase(phase), duration_us);
    })));
}

/// Clear the global marker sink (server shutdown).
pub fn clear_usdt_marker_sink() {
    kaya_core::set_probe_marker_callback(None);
}
