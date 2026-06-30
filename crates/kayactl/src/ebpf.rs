use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use kaya_core::Result;
use kaya_ebpf::{filter_wal_events, ProbeStatus};

const LINUX_ONLY_MSG: &str =
    "In-process eBPF probes are Linux-only. Use bpftrace scripts in scripts/ebpf/ on any Linux host.";

/// Handle `kayactl ebpf ...` subcommands.
pub(crate) fn handle_ebpf(sub: &str, data_dir: &str, pid: Option<u32>, _json: bool) -> Result<()> {
    match sub {
        "status" => print_status(data_dir, pid),
        "help" | "" => print_help(),
        _ => print_help(),
    }
}

/// `kayactl ebpf trace wal`
pub(crate) fn handle_ebpf_trace(sub: &str, data_dir: &str, _json: bool) -> Result<()> {
    match sub {
        "wal" => print_trace_wal(data_dir),
        _ => Err(kaya_core::KayaError::invalid_argument(
            "usage: kayactl ebpf trace wal [--data <dir>]",
        )),
    }
}

fn print_help() -> Result<()> {
    println!("kayactl ebpf — optional in-process observability (kaya-ebpf)");
    println!();
    println!("Subcommands:");
    println!("  kayactl ebpf status [--data <dir>] [--pid <pid>]   Probe attachment + streaming state");
    println!("  kayactl ebpf trace wal [--data <dir>]              WAL-relevant lines from trace.jsonl");
    println!("  kayactl ebpf help");
    println!();
    println!("Enable probes on the server: kayadb-server --ebpf [--ebpf-seed N]");
    println!("External bpftrace scripts: scripts/ebpf/ (see README.md)");
    if !cfg!(target_os = "linux") {
        println!();
        println!("{LINUX_ONLY_MSG}");
    }
    Ok(())
}

fn print_status(data_dir: &str, pid: Option<u32>) -> Result<()> {
    if !cfg!(target_os = "linux") {
        println!("{LINUX_ONLY_MSG}");
        println!("Scripts: scripts/ebpf/");
        return Ok(());
    }

    let status_path = Path::new(data_dir).join("ebpf/status.json");
    if status_path.exists() {
        let raw = std::fs::read_to_string(&status_path)
            .map_err(|e| kaya_core::KayaError::Io { message: e.to_string() })?;
        let status: ProbeStatus = serde_json::from_str(&raw).map_err(|e| {
            kaya_core::KayaError::invalid_argument(format!("invalid ebpf status json: {e}"))
        })?;
        println!("eBPF status (from {}):", status_path.display());
        println!("  attached:          {}", status.attached);
        println!("  streaming:         {}", status.streaming);
        println!("  backend:           {}", status.backend);
        println!("  events_collected:  {}", status.events_collected);
        println!("  seed:              {}", status.seed);
        println!("  trace_path:        {}", status.trace_path);
        return Ok(());
    }

    if let Some(pid) = pid.or_else(detect_server_pid) {
        println!("eBPF status for PID {pid}:");
        println!("  attached:          unknown (no {}/ebpf/status.json)", data_dir);
        println!("  streaming:         unknown");
        println!("  hint: start server with --ebpf to populate status artifacts");
    } else {
        println!("eBPF status: no local kayadb-server detected");
        println!("  hint: kayadb-server --ebpf --data {data_dir}");
    }
    println!("Scripts: scripts/ebpf/");
    Ok(())
}

fn print_trace_wal(data_dir: &str) -> Result<()> {
    if !cfg!(target_os = "linux") {
        println!("{LINUX_ONLY_MSG}");
        println!("Scripts: scripts/ebpf/");
        return Ok(());
    }

    let trace_path = Path::new(data_dir).join("ebpf/trace.jsonl");
    if !trace_path.exists() {
        println!(
            "No trace at {}. Run kayadb-server --ebpf and drive WAL traffic first.",
            trace_path.display()
        );
        return Ok(());
    }

    let file = File::open(&trace_path)
        .map_err(|e| kaya_core::KayaError::Io { message: e.to_string() })?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| kaya_core::KayaError::Io { message: e.to_string() })?;
        if idx == 0 && line.contains("artifact") {
            println!("# {line}");
            continue;
        }
        if line.trim().is_empty() {
            continue;
        }
        if let Ok(event) = serde_json::from_str(&line) {
            events.push(event);
        }
    }

    println!("WAL-relevant eBPF trace lines from {}:", trace_path.display());
    for event in filter_wal_events(&events) {
        println!("{}", serde_json::to_string(event).unwrap_or_default());
    }
    if events.is_empty() {
        println!("(no events recorded yet)");
    }
    Ok(())
}

fn detect_server_pid() -> Option<u32> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    std::process::Command::new("pgrep")
        .args(["-f", "kayadb-server"])
        .output()
        .ok()
        .and_then(|o| {
            String::from_utf8(o.stdout)
                .ok()
                .and_then(|s| s.lines().next().and_then(|l| l.trim().parse().ok()))
        })
}

