//! PUT + auto-flush drives OTel durability spans (`wal_fsync`, `flush`).

#![cfg(all(feature = "ebpf", feature = "otel"))]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use kaya_net::{encode_put_payload, roundtrip};
use kaya_server::cluster::{ClusterConfig, ClusterNode};
use kaya_server::otel_spans::{
    flush_durability_spans, install_durability_span_exporter, provider_with_exporter,
    shutdown_durability_spans, spans_summary,
};
use opentelemetry_sdk::trace::InMemorySpanExporter;
use serial_test::serial;
use tokio::net::TcpListener;

fn goal_scratch_dir() -> PathBuf {
    std::env::var("KAYA_GOAL_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(r"C:\Users\tunay\AppData\Local\Temp\grok-goal-0c68bfec5b45\implementer")
        })
}

async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

fn assert_durability_span(
    spans: &[opentelemetry_sdk::trace::SpanData],
    name: &str,
) -> opentelemetry_sdk::trace::SpanData {
    let span = spans
        .iter()
        .find(|s| s.name.as_ref() == name)
        .unwrap_or_else(|| {
            panic!(
                "missing {name} span; names={:?}",
                spans.iter().map(|s| &s.name).collect::<Vec<_>>()
            )
        });
    let phase = span
        .attributes
        .iter()
        .find(|kv| kv.key.as_ref() == "kaya.durability.phase")
        .map(|kv| kv.value.as_str().into_owned())
        .unwrap_or_default();
    assert_eq!(phase, "enter", "{name} must retain enter phase on export");
    let duration_us = span
        .attributes
        .iter()
        .find(|kv| kv.key.as_ref() == "kaya.durability.duration_us")
        .and_then(|kv| match kv.value {
            opentelemetry::Value::I64(v) => Some(v),
            _ => None,
        });
    assert!(
        duration_us.is_some_and(|us| us > 0),
        "{name} must export duration_us > 0; attrs={:?}",
        span.attributes
    );
    let events: Vec<String> = span
        .events
        .events
        .iter()
        .map(|e| e.name.to_string())
        .collect();
    assert_eq!(events, vec!["enter", "exit"], "{name} enter/exit events");
    let wall_ns = span
        .end_time
        .duration_since(span.start_time)
        .unwrap_or_default()
        .as_nanos();
    assert!(wall_ns > 0, "{name} wall-clock span duration must be > 0");
    span.clone()
}

#[serial]
#[tokio::test]
async fn otel_spans_export_wal_fsync_and_flush_after_put() {
    let exporter = InMemorySpanExporter::default();
    install_durability_span_exporter(provider_with_exporter(exporter.clone()));

    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!("kayadb_otel_{test_id}"));
    let client_port = free_port().await;
    let raft_port = free_port().await;
    let client_addr: SocketAddr = format!("127.0.0.1:{client_port}").parse().unwrap();
    let raft_addr: SocketAddr = format!("127.0.0.1:{raft_port}").parse().unwrap();

    let config = ClusterConfig::new(1, &data_dir, raft_addr, client_addr, vec![])
        .with_ebpf(42)
        .with_otel();
    let handle = tokio::spawn(async move {
        let _ = ClusterNode::new(config).run().await;
    });

    let mut ready = false;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if let Ok((status, body)) = roundtrip(client_addr, 5, &[]).await {
            if status == 0 && body == b"leader" {
                ready = true;
                break;
            }
        }
    }
    assert!(ready, "node did not become leader");

    let put = encode_put_payload(b"otel-key", b"otel-value");
    let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
    assert_eq!(status, 0);

    tokio::time::sleep(Duration::from_millis(500)).await;
    handle.abort();
    flush_durability_spans();

    let spans = exporter.get_finished_spans().expect("finished spans");
    let _wal = assert_durability_span(&spans, "wal_fsync");
    let _flush = assert_durability_span(&spans, "flush");
    let summary = spans_summary(&spans);
    shutdown_durability_spans();

    assert!(summary.contains("\"phase\":\"enter\""));
    assert!(summary.contains("\"duration_us\":"));
    assert!(summary.contains("\"events\":[\"enter\", \"exit\"]"));

    let scratch = goal_scratch_dir();
    let _ = std::fs::create_dir_all(&scratch);
    std::fs::write(scratch.join("otel-spans-smoke.json"), &summary).unwrap();

    let _ = std::fs::remove_dir_all(&data_dir);
}
