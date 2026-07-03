//! Shared helpers for eBPF server integration evidence (trace + correlate).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

pub fn goal_scratch_dir() -> PathBuf {
    std::env::var("KAYA_GOAL_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(r"C:\Users\tunay\AppData\Local\Temp\grok-goal-10c42b461488\implementer")
        })
}

pub fn kayactl_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/kayactl.exe")
}

pub fn assert_server_trace_marker_kinds(trace_raw: &str) {
    assert!(
        trace_raw.contains("\"kind\":\"usdt_marker\""),
        "trace must contain usdt_marker events; got:\n{trace_raw}"
    );
    assert!(
        trace_raw.contains("\"site\":\"wal_fsync\""),
        "trace must contain wal_fsync markers"
    );
    assert!(
        trace_raw.contains("\"site\":\"flush\""),
        "trace must contain flush markers (auto-flush after PUT)"
    );
    assert!(
        trace_raw.contains("\"phase\":\"enter\"") && trace_raw.contains("\"phase\":\"exit\""),
        "trace must contain balanced enter/exit marker phases"
    );
    assert!(
        trace_raw.contains("\"kind\":\"publish_syscall\""),
        "trace must contain publish_syscall events from flush exit"
    );
    assert!(
        trace_raw.contains("\"syscall\":\"rename\"")
            || trace_raw.contains("\"syscall\":\"fsync_dir\""),
        "trace must name publish syscall kinds"
    );
    assert!(
        trace_raw.contains("\"kind\":\"fsync_latency\""),
        "trace must retain legacy fsync_latency kernel events"
    );
}

pub async fn wait_for_server_trace(data_dir: &Path) -> String {
    let trace_path = data_dir.join("ebpf/trace.jsonl");
    for _ in 0..80 {
        if trace_path.is_file() {
            let raw = std::fs::read_to_string(&trace_path).unwrap_or_default();
            if raw.contains("\"site\":\"wal_fsync\"")
                && raw.contains("\"site\":\"flush\"")
                && raw.contains("\"kind\":\"publish_syscall\"")
            {
                return raw;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!(
        "timed out waiting for server trace markers at {}",
        trace_path.display()
    );
}

pub fn capture_server_correlate(data_dir: &Path, scratch: &Path, run: u32) {
    assert!(
        kayactl_bin().exists(),
        "build kayactl first: cargo build -p kayactl --features ebpf"
    );
    let output = Command::new(kayactl_bin())
        .args([
            "ebpf",
            "correlate",
            "--data",
            &data_dir.display().to_string(),
            "--durability",
            "strict",
        ])
        .output()
        .expect("spawn kayactl ebpf correlate");
    assert!(
        output.status.success(),
        "kayactl correlate failed (stop server before correlate to release KAYA_LOCK): stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let rendered = String::from_utf8_lossy(&output.stdout).to_string();
    assert!(
        rendered.contains("USDT markers"),
        "correlate must list USDT markers"
    );
    assert!(
        rendered.contains("wal_enter="),
        "correlate must count wal markers"
    );
    assert!(
        rendered.contains("flush_enter="),
        "correlate must count flush markers"
    );
    assert!(
        rendered.contains("Publish trace"),
        "correlate must list publish trace"
    );
    assert!(
        rendered.contains("rename"),
        "correlate must name publish kinds"
    );
    std::fs::write(scratch.join(format!("correlate-run-{run}.txt")), &rendered).unwrap();
}
