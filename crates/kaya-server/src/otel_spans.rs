//! Optional OpenTelemetry spans at WAL fsync and flush durability boundaries (Phase 2C).

use std::sync::{Mutex, OnceLock};

use kaya_core::{set_probe_span_callback, ProbeMarkerPhase, ProbeMarkerSite};
use opentelemetry::global::{self, BoxedSpan, BoxedTracer};
use opentelemetry::trace::{Span, Tracer};
use opentelemetry::KeyValue;
use opentelemetry_sdk::trace::SdkTracerProvider;
use parking_lot::Mutex as ParkingMutex;

static PROVIDER: OnceLock<ParkingMutex<Option<SdkTracerProvider>>> = OnceLock::new();

fn provider_slot() -> &'static ParkingMutex<Option<SdkTracerProvider>> {
    PROVIDER.get_or_init(|| ParkingMutex::new(None))
}

struct ActiveDurabilitySpans {
    wal_fsync: Option<BoxedSpan>,
    flush: Option<BoxedSpan>,
}

fn active_spans() -> &'static Mutex<ActiveDurabilitySpans> {
    static SLOT: OnceLock<Mutex<ActiveDurabilitySpans>> = OnceLock::new();
    SLOT.get_or_init(|| {
        Mutex::new(ActiveDurabilitySpans {
            wal_fsync: None,
            flush: None,
        })
    })
}

fn tracer() -> BoxedTracer {
    global::tracer("kaya-server")
}

fn handle_durability_span(
    site: ProbeMarkerSite,
    phase: ProbeMarkerPhase,
    duration_us: Option<u64>,
) {
    let mut active = active_spans().lock().expect("active spans mutex poisoned");
    let slot = match site {
        ProbeMarkerSite::WalFsync => &mut active.wal_fsync,
        ProbeMarkerSite::Flush => &mut active.flush,
    };
    match phase {
        ProbeMarkerPhase::Enter => {
            let mut span = tracer().start(site.as_str());
            span.set_attribute(KeyValue::new("kaya.durability.site", site.as_str()));
            span.set_attribute(KeyValue::new("kaya.durability.phase", "enter"));
            *slot = Some(span);
        }
        ProbeMarkerPhase::Exit => {
            if let Some(mut span) = slot.take() {
                span.set_attribute(KeyValue::new("kaya.durability.phase", "exit"));
                if let Some(us) = duration_us {
                    span.set_attribute(KeyValue::new("kaya.durability.duration_us", us as i64));
                }
                span.end();
            }
        }
    }
}

/// Install OTel-backed durability span sink and register global tracer provider.
pub fn install_durability_span_exporter(provider: SdkTracerProvider) {
    global::set_tracer_provider(provider.clone());
    *provider_slot().lock() = Some(provider);
    set_probe_span_callback(Some(Box::new(handle_durability_span)));
}

/// Whether a tracer provider is already installed.
pub fn provider_slot_is_empty() -> bool {
    provider_slot().lock().is_none()
}

/// Install stdout-exporting durability spans (used by `kayadb-server --otel`).
pub fn install_default_durability_spans() {
    let exporter = opentelemetry_stdout::SpanExporter::default();
    install_durability_span_exporter(provider_with_exporter(exporter));
}

/// Flush pending spans to the configured exporter.
pub fn flush_durability_spans() {
    if let Some(provider) = provider_slot().lock().as_ref() {
        let _ = provider.force_flush();
    }
}

/// Clear span sink and shut down the tracer provider.
pub fn shutdown_durability_spans() {
    set_probe_span_callback(None);
    if let Some(provider) = provider_slot().lock().take() {
        if let Err(err) = provider.shutdown() {
            eprintln!("otel provider shutdown: {err}");
        }
    }
}

/// Build a simple SDK provider with the given span exporter (for tests or file export).
pub fn provider_with_exporter<E>(exporter: E) -> SdkTracerProvider
where
    E: opentelemetry_sdk::trace::SpanExporter + Send + Sync + 'static,
{
    SdkTracerProvider::builder()
        .with_simple_exporter(exporter)
        .build()
}

/// Serialize finished span names for evidence dumps (tests / scratch artifacts).
pub fn spans_summary(spans: &[opentelemetry_sdk::trace::SpanData]) -> String {
    let names: Vec<String> = spans.iter().map(|s| s.name.to_string()).collect();
    format!("{{\"span_names\":{names:?}}}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_core::emit_probe_marker;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};

    fn finished_span_names(spans: &[SpanData]) -> Vec<String> {
        spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
    }

    #[test]
    fn durability_spans_record_wal_fsync_and_flush_enter_exit() {
        let exporter = InMemorySpanExporter::default();
        let provider = provider_with_exporter(exporter.clone());
        install_durability_span_exporter(provider);

        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Enter, None);
        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Exit, Some(120));
        emit_probe_marker(ProbeMarkerSite::Flush, ProbeMarkerPhase::Enter, None);
        emit_probe_marker(ProbeMarkerSite::Flush, ProbeMarkerPhase::Exit, Some(45_000));

        flush_durability_spans();
        let names = finished_span_names(&exporter.get_finished_spans().unwrap());
        shutdown_durability_spans();
        assert!(
            names.iter().filter(|n| n.as_str() == "wal_fsync").count() >= 1,
            "expected wal_fsync span, got: {names:?}"
        );
        assert!(
            names.iter().filter(|n| n.as_str() == "flush").count() >= 1,
            "expected flush span, got: {names:?}"
        );
    }

    #[test]
    fn span_sink_is_noop_without_install() {
        kaya_core::set_probe_span_callback(None);
        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Enter, None);
        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Exit, Some(1));
    }
}
