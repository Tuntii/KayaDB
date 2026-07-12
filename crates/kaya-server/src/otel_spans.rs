//! Optional OpenTelemetry spans at WAL fsync and flush durability boundaries (Phase 2C).
//!
//! **M20 multi-raft tracing stub:** when multi-raft host paths are wired through
//! ClusterNode, attach attribute kaya.raft.group_id on propose/handle/apply
//! spans so node↔node↔client traces demux by group. Full W3C trace-context
//! propagation remains follow-on (M20 v1 / M24 full).

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
            span.add_event("enter", vec![]);
            *slot = Some(span);
        }
        ProbeMarkerPhase::Exit => {
            if let Some(mut span) = slot.take() {
                if let Some(us) = duration_us {
                    span.set_attribute(KeyValue::new("kaya.durability.duration_us", us as i64));
                }
                let exit_attrs = duration_us
                    .map(|us| vec![KeyValue::new("kaya.durability.duration_us", us as i64)])
                    .unwrap_or_default();
                span.add_event("exit", exit_attrs);
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

fn attr_string(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<String> {
    span.attributes.iter().find_map(|kv| {
        if kv.key.as_ref() == key {
            Some(kv.value.as_str().into_owned())
        } else {
            None
        }
    })
}

fn attr_i64(span: &opentelemetry_sdk::trace::SpanData, key: &str) -> Option<i64> {
    span.attributes.iter().find_map(|kv| {
        if kv.key.as_ref() == key {
            match kv.value {
                opentelemetry::Value::I64(v) => Some(v),
                _ => None,
            }
        } else {
            None
        }
    })
}

fn wall_duration_ns(span: &opentelemetry_sdk::trace::SpanData) -> u128 {
    span.end_time
        .duration_since(span.start_time)
        .unwrap_or_default()
        .as_nanos()
}

fn event_names(span: &opentelemetry_sdk::trace::SpanData) -> Vec<String> {
    span.events
        .events
        .iter()
        .map(|e| e.name.to_string())
        .collect()
}

/// Serialize finished spans for evidence dumps (tests / scratch artifacts).
pub fn spans_summary(spans: &[opentelemetry_sdk::trace::SpanData]) -> String {
    let mut lines = Vec::new();
    lines.push("[".to_owned());
    for (idx, span) in spans.iter().enumerate() {
        if idx > 0 {
            lines.push(",".to_owned());
        }
        let events = event_names(span);
        lines.push(format!(
            "{{\"name\":\"{}\",\"phase\":\"{}\",\"duration_us\":{},\"wall_duration_ns\":{},\"events\":{:?}}}",
            span.name,
            attr_string(span, "kaya.durability.phase").unwrap_or_default(),
            attr_i64(span, "kaya.durability.duration_us")
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_owned()),
            wall_duration_ns(span),
            events,
        ));
    }
    lines.push("]".to_owned());
    lines.join("\n")
}

#[cfg(test)]
pub(crate) fn span_site_phase_duration(
    span: &opentelemetry_sdk::trace::SpanData,
) -> (String, String, Option<i64>, u128, Vec<String>) {
    (
        span.name.to_string(),
        attr_string(span, "kaya.durability.phase").unwrap_or_default(),
        attr_i64(span, "kaya.durability.duration_us"),
        wall_duration_ns(span),
        event_names(span),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_core::emit_probe_marker;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SpanData};

    fn finished_spans(exporter: &InMemorySpanExporter) -> Vec<SpanData> {
        flush_durability_spans();
        exporter.get_finished_spans().unwrap()
    }

    fn find_span<'a>(spans: &'a [SpanData], name: &str) -> &'a SpanData {
        spans
            .iter()
            .find(|s| s.name.as_ref() == name)
            .unwrap_or_else(|| {
                panic!(
                    "missing span {name}; got {:?}",
                    spans.iter().map(|s| &s.name).collect::<Vec<_>>()
                )
            })
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

        let spans = finished_spans(&exporter);
        shutdown_durability_spans();

        let wal = find_span(&spans, "wal_fsync");
        let (name, phase, duration_us, wall_ns, events) = span_site_phase_duration(wal);
        assert_eq!(name, "wal_fsync");
        assert_eq!(phase, "enter", "finished span must retain enter phase");
        assert_eq!(duration_us, Some(120));
        assert!(
            wall_ns > 0,
            "wal_fsync span must have non-zero wall duration"
        );
        assert_eq!(events, vec!["enter", "exit"]);

        let flush = find_span(&spans, "flush");
        let (_, phase, duration_us, wall_ns, events) = span_site_phase_duration(flush);
        assert_eq!(phase, "enter");
        assert_eq!(duration_us, Some(45_000));
        assert!(wall_ns > 0);
        assert_eq!(events, vec!["enter", "exit"]);

        let summary = spans_summary(&spans);
        assert!(summary.contains("\"phase\":\"enter\""));
        assert!(summary.contains("\"duration_us\":120"));
        assert!(summary.contains("\"duration_us\":45000"));
    }

    #[test]
    fn span_sink_is_noop_without_install() {
        kaya_core::set_probe_span_callback(None);
        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Enter, None);
        emit_probe_marker(ProbeMarkerSite::WalFsync, ProbeMarkerPhase::Exit, Some(1));
    }
}
