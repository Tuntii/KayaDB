//! Launch real `kayadb-server --ebpf` and verify non-zero eBPF Prometheus samples.

#![cfg(feature = "ebpf")]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

fn goal_scratch_dir() -> PathBuf {
    std::env::var("KAYA_GOAL_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(r"C:\Users\tunay\AppData\Local\Temp\grok-goal-e9b62b239508\implementer")
        })
}

use kaya_net::{encode_put_payload, roundtrip};
use serial_test::serial;
use tokio::net::TcpListener;

fn server_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug/kayadb-server.exe")
}

async fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

fn prometheus_sample(body: &str, prefix: &str) -> Option<u64> {
    body.lines()
        .find(|l| l.starts_with(prefix) && !l.contains("#"))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

fn spawn_server(data_dir: &PathBuf, client: SocketAddr, raft: SocketAddr, metrics: SocketAddr) -> Child {
    Command::new(server_bin())
        .args([
            "--node-id",
            "1",
            "--data",
            &data_dir.display().to_string(),
            "--client-addr",
            &client.to_string(),
            "--raft-addr",
            &raft.to_string(),
            "--metrics-addr",
            &metrics.to_string(),
            "--ebpf",
            "--ebpf-seed",
            "42",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn kayadb-server")
}

#[serial]
#[tokio::test]
async fn kayadb_server_bin_ebpf_metrics_nonzero_after_put() {
    assert!(
        server_bin().exists(),
        "build kayadb-server first: cargo build -p kaya-server --features ebpf --bin kayadb-server"
    );

    let test_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let data_dir = std::env::temp_dir().join(format!("kayadb_bin_ebpf_{test_id}"));
    let client_port = free_port().await;
    let raft_port = free_port().await;
    let metrics_port = free_port().await;
    let client_addr: SocketAddr = format!("127.0.0.1:{client_port}").parse().unwrap();
    let raft_addr: SocketAddr = format!("127.0.0.1:{raft_port}").parse().unwrap();
    let metrics_addr: SocketAddr = format!("127.0.0.1:{metrics_port}").parse().unwrap();

    let mut child = spawn_server(&data_dir, client_addr, raft_addr, metrics_addr);

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
    assert!(ready, "server failed to become leader");

    let put = encode_put_payload(b"bin-ebpf-key", b"bin-ebpf-val");
    let (status, _) = roundtrip(client_addr, 1, &put).await.unwrap();
    assert_eq!(status, 0);

    let mut saw_nonzero = false;
    for _ in 0..80 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut stream = tokio::net::TcpStream::connect(metrics_addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 32_768];
        let n = stream.read(&mut buf).await.unwrap();
        let body = String::from_utf8_lossy(&buf[..n]);
        let count = prometheus_sample(&body, "kaya_ebpf_fsync_latency_us_count{syscall=\"fsync\"}")
            .unwrap_or(0);
        let wal = prometheus_sample(&body, "kaya_wal_fsync_total_us").unwrap_or(0);
        if count > 0 && wal > 0 {
            saw_nonzero = true;
            break;
        }
    }
    assert!(saw_nonzero, "bin launch: timed out waiting for non-zero ebpf metrics");

    let scratch = goal_scratch_dir();
    let _ = std::fs::create_dir_all(&scratch);

    for run in 0..2 {
        let mut stream = tokio::net::TcpStream::connect(metrics_addr).await.unwrap();
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        stream
            .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 32_768];
        let n = stream.read(&mut buf).await.unwrap();
        let body = String::from_utf8_lossy(&buf[..n]).to_string();
        std::fs::write(scratch.join(format!("bin-metrics-scrape-{run}.txt")), &body).unwrap();
        if run == 0 {
            std::fs::write(scratch.join("ebpf-metrics-integration.log"), &body).unwrap();
        }

        let count = prometheus_sample(&body, "kaya_ebpf_fsync_latency_us_count{syscall=\"fsync\"}")
            .unwrap_or(0);
        let sum = prometheus_sample(&body, "kaya_ebpf_fsync_latency_us_sum{syscall=\"fsync\"}")
            .unwrap_or(0);
        assert!(count > 0, "bin launch: eBPF count must be >0\n{body}");
        assert!(sum > 0, "bin launch: eBPF sum must be >0");
        assert!(
            body.lines().any(|l| {
                l.starts_with("kaya_ebpf_fsync_latency_us_bucket{syscall=\"fsync\"")
                    && !l.ends_with("} 0")
            }),
            "bin launch: expected non-zero eBPF bucket\n{body}"
        );
        assert!(
            body.contains("kernel-slot fsync latency"),
            "metrics HELP must describe kernel-slot backend, not userspace-tap\n{body}"
        );
        assert!(
            !body.contains("userspace-tap"),
            "metrics must not reference legacy userspace-tap HELP\n{body}"
        );
    }

    let status_path = data_dir.join("ebpf/status.json");
    let mut status_ready = false;
    for _ in 0..40 {
        if status_path.exists() {
            let status_raw = std::fs::read_to_string(&status_path).unwrap();
            if status_raw.contains("kernel") {
                status_ready = true;
                std::fs::write(scratch.join("ebpf-status.json"), &status_raw).unwrap();
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        status_ready,
        "ebpf/status.json must exist with kernel-family backend after PUT"
    );

    let trace_path = data_dir.join("ebpf/trace.jsonl");
    if trace_path.exists() {
        let trace_raw = std::fs::read_to_string(&trace_path).unwrap();
        assert!(
            trace_raw.contains("ts_ns") || trace_raw.contains("latency_us"),
            "trace.jsonl should contain kernel-shaped durability events"
        );
    }

    let fallback_note = format!(
        "host={} os={}\n\
         backend_slot=kernel-simulated (ProbeConfig::for_server on non-Linux; KernelLive needs Linux+kernel-probes+CAP_BPF)\n\
         metrics_help=kernel-slot (not userspace-tap)\n\
         evidence=bin-metrics-scrape-0.txt bin-metrics-scrape-1.txt ebpf-metrics-integration.log ebpf-status.json\n\
         server_bin={}\n",
        std::env::consts::ARCH,
        std::env::consts::OS,
        server_bin().display()
    );
    std::fs::write(scratch.join("ebpf-launch-fallback.log"), fallback_note).unwrap();
    assert!(scratch.join("ebpf-metrics-integration.log").exists());
    assert!(Path::new(&scratch.join("bin-metrics-scrape-0.txt")).exists());

    let _ = child.kill();
    let _ = std::fs::remove_dir_all(&data_dir);
}