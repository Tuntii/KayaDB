use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

use kaya_core::Result;
use kaya_ebpf::{filter_publish_events, filter_wal_events, ProbeStatus};

use crate::ebpf_bpftrace::{
    discover_server_pids, format_catalog_script_names, list_active_bpftrace, resolve_script,
    run_bpftrace_script, run_flamegraph_helper, server_pid_details,
};
use crate::ebpf_correlate::{correlate_report, print_correlate_human};

const LINUX_ONLY_MSG: &str =
    "In-process eBPF probes are Linux-only. Use bpftrace scripts in scripts/ebpf/ on any Linux host.";

/// Handle `kayactl ebpf ...` subcommands.
pub(crate) fn handle_ebpf(
    sub: &str,
    data_dir: &str,
    pid: Option<u32>,
    _json: bool,
    run: bool,
    duration_secs: u64,
    durability: kaya_core::DurabilityMode,
) -> Result<()> {
    match sub {
        "list" => print_list(),
        "status" => print_status(data_dir, pid),
        "correlate" => {
            let report = correlate_report(data_dir, durability)?;
            print_correlate_human(&report);
            Ok(())
        }
        "fsync-latency" => run_bpftrace_script(pid, "fsync-latency", run, duration_secs),
        "block-latency" => run_bpftrace_script(pid, "block-latency", run, duration_secs),
        "syscall-timeline" => run_bpftrace_script(pid, "syscall-timeline", run, duration_secs),
        "flamegraph" => run_flamegraph_helper(pid, run, duration_secs),
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
    println!("  kayactl ebpf list                                              Discover server PIDs + catalog scripts");
    println!("  kayactl ebpf status [--data <dir>] [--pid <pid>]               Probe attachment + streaming state");
    println!("  kayactl ebpf trace wal [--data <dir>]                          WAL-relevant lines from trace.jsonl");
    println!("  kayactl ebpf correlate [--data <dir>]                            Userspace WAL vs kernel trace summary");
    println!("  kayactl ebpf fsync-latency [--pid <pid>] [--run] [--duration <sec>]");
    println!("  kayactl ebpf block-latency [--pid <pid>] [--run] [--duration <sec>]");
    println!("  kayactl ebpf syscall-timeline [--pid <pid>] [--run] [--duration <sec>]");
    println!("  kayactl ebpf flamegraph [--pid <pid>] [--run] [--duration <sec>]  Stack-collapse / flamegraph helper");
    println!("  kayactl ebpf help");
    println!();
    println!("bpftrace wrapper (--run spawns bpftrace for up to --duration seconds, default 10):");
    println!("  Without --run: prints sudo bpftrace -p <PID> <script> (no bpftrace required)");
    println!();
    println!("Enable probes on the server: kayadb-server --ebpf [--ebpf-seed N]");
    println!("External bpftrace scripts: scripts/ebpf/ (see README.md)");
    if !cfg!(target_os = "linux") {
        println!();
        println!("{LINUX_ONLY_MSG}");
    }
    Ok(())
}

fn print_list() -> Result<()> {
    if !cfg!(target_os = "linux") {
        println!("{LINUX_ONLY_MSG}");
    }

    let server_details = server_pid_details();
    println!("kayadb-server PIDs:");
    if server_details.is_empty() {
        println!("  (none)");
    } else {
        for (pid, cmd) in server_details {
            println!("  {pid}  cmd={cmd}");
        }
    }

    let bpftrace_pids = list_active_bpftrace();
    if bpftrace_pids.is_empty() {
        println!("bpftrace processes: (none)");
    } else {
        let pids = bpftrace_pids
            .iter()
            .map(|pid| pid.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        println!("bpftrace processes: {pids}");
    }

    let catalog = format_catalog_script_names();
    if catalog.is_empty() {
        println!("catalog scripts: (none)");
    } else {
        println!("catalog scripts: {catalog}");
    }

    Ok(())
}

fn print_status(data_dir: &str, pid: Option<u32>) -> Result<()> {
    if !cfg!(target_os = "linux") {
        println!("{LINUX_ONLY_MSG}");
        print_scripts_hint();
        return Ok(());
    }

    let status_path = Path::new(data_dir).join("ebpf/status.json");
    if status_path.exists() {
        let raw = std::fs::read_to_string(&status_path).map_err(|e| kaya_core::KayaError::Io {
            message: e.to_string(),
        })?;
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

    if let Some(pid) = pid.or_else(|| discover_server_pids().first().copied()) {
        println!("eBPF status for PID {pid}:");
        println!(
            "  attached:          unknown (no {}/ebpf/status.json)",
            data_dir
        );
        println!("  streaming:         unknown");
        println!("  hint: start server with --ebpf to populate status artifacts");
    } else {
        println!("eBPF status: no local kayadb-server detected");
        println!("  hint: kayadb-server --ebpf --data {data_dir}");
    }
    let bpftrace_pids = list_active_bpftrace();
    if !bpftrace_pids.is_empty() {
        println!("  active bpftrace:   {bpftrace_pids:?}");
    }
    print_scripts_hint();
    Ok(())
}

fn print_scripts_hint() {
    match resolve_script("fsync-latency") {
        Ok(path) => {
            let dir = path
                .parent()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "scripts/ebpf".to_owned());
            println!("Scripts: {dir}/");
        }
        Err(_) => println!("Scripts: scripts/ebpf/"),
    }
}

fn print_trace_wal(data_dir: &str) -> Result<()> {
    if !cfg!(target_os = "linux") {
        println!("{LINUX_ONLY_MSG}");
        print_scripts_hint();
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

    let file = File::open(&trace_path).map_err(|e| kaya_core::KayaError::Io {
        message: e.to_string(),
    })?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| kaya_core::KayaError::Io {
            message: e.to_string(),
        })?;
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

    println!(
        "WAL-relevant eBPF trace lines from {}:",
        trace_path.display()
    );
    let wal = filter_wal_events(&events);
    for event in wal {
        println!("{}", serde_json::to_string(event).unwrap_or_default());
    }
    let publish = filter_publish_events(&events);
    if !publish.is_empty() {
        println!();
        println!("Flush publish lines (flush markers + publish_syscall):");
        for event in publish {
            println!("{}", serde_json::to_string(event).unwrap_or_default());
        }
    }
    if events.is_empty() {
        println!("(no events recorded yet)");
    }
    Ok(())
}
