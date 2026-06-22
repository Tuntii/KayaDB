use kaya_core::Result;

/// Handle `kayactl ebpf ...` subcommands.
/// Linux eBPF observability experiments (Track A / M12). Non-Linux and missing tools are handled gracefully.
/// If `run` is true we attempt to exec bpftrace (requires appropriate privileges on Linux).
/// `duration` (e.g. "30s") is best-effort for --run (uses `timeout` on Unix if available).
pub(crate) fn handle_ebpf(
    sub: &str,
    explicit_pid: Option<u32>,
    run: bool,
    duration: Option<String>,
    _json: bool,
) -> Result<()> {
    const LINUX_ONLY_MSG: &str = "eBPF probes are Linux-only. This command provides ready-to-run bpftrace commands and guidance (see scripts/ebpf/).";

    if !cfg!(target_os = "linux") {
        println!("{}", LINUX_ONLY_MSG);
        println!(
            "Scripts live in scripts/ebpf/ and work on any Linux machine with bpftrace + sudo."
        );
        return Ok(());
    }

    // Try to auto-detect a KayaDB server PID if none supplied.
    let pid = explicit_pid.or_else(|| {
        // Best-effort: look for kayadb-server first, then kayactl (for local engine tests)
        std::process::Command::new("pgrep")
            .args(["-f", "kayadb-server"])
            .output()
            .ok()
            .and_then(|o| {
                String::from_utf8(o.stdout)
                    .ok()
                    .and_then(|s| s.lines().next().and_then(|l| l.trim().parse::<u32>().ok()))
            })
            .or_else(|| {
                std::process::Command::new("pgrep")
                    .args(["-f", "kayactl"])
                    .output()
                    .ok()
                    .and_then(|o| {
                        String::from_utf8(o.stdout).ok().and_then(|s| {
                            s.lines().next().and_then(|l| l.trim().parse::<u32>().ok())
                        })
                    })
            })
    });

    let pid_str = pid
        .map(|p| p.to_string())
        .unwrap_or_else(|| "<PID>".to_string());
    let pid_hint = if pid.is_some() {
        format!("(auto-detected PID {})", pid_str)
    } else {
        "(pass --pid <N> or run against a live kayadb-server)".to_string()
    };

    match sub {
        "fsync-latency" | "fsync" => {
            println!("KayaDB eBPF: fsync / fdatasync latency (microseconds)");
            println!("PID hint: {}", pid_hint);
            println!();
            println!("Recommended (copy-paste):");
            println!(
                "  sudo bpftrace -p {} scripts/ebpf/fsync-latency.bt",
                pid_str
            );
            println!();
            println!("Alternative one-liner:");
            println!("  sudo bpftrace -e '");
            println!("    kprobe:sys_fsync, kprobe:sys_fdatasync {{ @start[tid] = nsecs; }}");
            println!("    kretprobe:sys_fsync, kretprobe:sys_fdatasync /@start[tid]/ {{");
            println!("      $us = (nsecs - @start[tid]) / 1000;");
            println!("      @fsync_us = hist($us); delete(@start[tid]);");
            println!("    }}' -p {}", pid_str);
            println!();
            println!("See: scripts/ebpf/README.md and spec/docs/observability-spec.md");
            // Attempt to run if bpftrace exists and we have a real PID (best effort, user will see permission errors)
            if pid.is_some()
                && std::process::Command::new("bpftrace")
                    .arg("--version")
                    .output()
                    .is_ok()
            {
                println!("\n[bpftrace found] You can now run the sudo command above.");
            }
            if run {
                return try_run_bpftrace("fsync-latency", &pid_str, duration.as_deref());
            }
            Ok(())
        }
        "block-latency" | "bio" | "block" => {
            println!("KayaDB eBPF: block I/O (device) latency histograms (us)");
            println!("PID hint: {}", pid_hint);
            println!();
            println!("Recommended (copy-paste):");
            println!(
                "  sudo bpftrace -p {} scripts/ebpf/block-io-latency.bt",
                pid_str
            );
            println!();
            println!("This shows time spent in the storage stack / scheduler / device after the fsync syscall.");
            println!("See: scripts/ebpf/README.md");
            if pid.is_some()
                && std::process::Command::new("bpftrace")
                    .arg("--version")
                    .output()
                    .is_ok()
            {
                println!("\n[bpftrace found] Ready to attach.");
            }
            if run {
                return try_run_bpftrace("block-io-latency", &pid_str, duration.as_deref());
            }
            Ok(())
        }
        "syscall-timeline" | "timeline" | "sys" => {
            println!("KayaDB eBPF: syscall timeline (write, fsync, fdatasync, rename, unlink, fsyncdir) + TID correlation");
            println!("PID hint: {}", pid_hint);
            println!();
            println!("Recommended (copy-paste):");
            println!(
                "  sudo bpftrace -p {} scripts/ebpf/syscall-timeline.bt",
                pid_str
            );
            println!();
            println!("This script correlates writes with their fsyncs by TID and shows rename/unlink for flush/compaction publish points.");
            println!("See: scripts/ebpf/README.md (Track A addition)");
            if pid.is_some()
                && std::process::Command::new("bpftrace")
                    .arg("--version")
                    .output()
                    .is_ok()
            {
                println!("\n[bpftrace found] Ready to attach.");
            }
            if run {
                return try_run_bpftrace("syscall-timeline", &pid_str, duration.as_deref());
            }
            Ok(())
        }
        "list" => {
            println!("kayactl ebpf list — active KayaDB / bpftrace processes (best effort)");
            if !cfg!(target_os = "linux") {
                println!("{}", LINUX_ONLY_MSG);
                return Ok(());
            }
            // List kayadb-server instances (cluster friendly)
            println!("\n--- kayadb-server processes ---");
            if let Ok(out) = std::process::Command::new("pgrep")
                .args(["-a", "kayadb-server"])
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.trim().is_empty() {
                    println!("  (none found)");
                } else {
                    for line in s.lines() {
                        println!("  {}", line.trim());
                    }
                }
            } else {
                println!("  pgrep unavailable");
            }
            // List kayactl (embedded tests)
            println!("\n--- kayactl processes ---");
            if let Ok(out) = std::process::Command::new("pgrep")
                .args(["-a", "kayactl"])
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.trim().is_empty() {
                    println!("  (none found)");
                } else {
                    for line in s.lines() {
                        println!("  {}", line.trim());
                    }
                }
            }
            // List running bpftrace traces (useful for "status" of traces)
            println!("\n--- bpftrace / trace processes ---");
            if let Ok(out) = std::process::Command::new("pgrep")
                .args(["-a", "bpftrace"])
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.trim().is_empty() {
                    println!("  (none found — no active traces)");
                } else {
                    for line in s.lines() {
                        println!("  {}", line.trim());
                    }
                }
            } else {
                println!("  pgrep bpftrace unavailable");
            }
            println!("\nUse the printed PIDs with --pid or the auto-detector.");
            Ok(())
        }
        "status" => {
            println!("kayactl ebpf status — quick view of local nodes + any attached traces");
            if !cfg!(target_os = "linux") {
                println!("{}", LINUX_ONLY_MSG);
                return Ok(());
            }
            // Reuse list logic but summarized
            let servers: Vec<String> = std::process::Command::new("pgrep")
                .args(["-f", "kayadb-server"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| {
                    s.lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(|l| l.trim().to_string())
                        .collect()
                })
                .unwrap_or_default();
            println!(
                "Detected kayadb-server PIDs: {}",
                if servers.is_empty() {
                    "(none)".into()
                } else {
                    servers.join(", ")
                }
            );
            let traces: Vec<String> = std::process::Command::new("pgrep")
                .args(["-a", "bpftrace"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| {
                    s.lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(|l| l.trim().to_string())
                        .collect()
                })
                .unwrap_or_default();
            println!(
                "Active bpftrace traces: {}",
                if traces.is_empty() {
                    "0 (no kernel probes attached)".into()
                } else {
                    traces.len().to_string()
                }
            );
            println!("Tip: kayactl ebpf list   for full details.");
            Ok(())
        }
        _ => {
            println!("kayactl ebpf — Linux eBPF observability experiments (Track A / M12)");
            println!();
            println!("Subcommands:");
            println!(
                "  kayactl ebpf fsync-latency [--pid <pid>] [--run]        Trace fsync/fdatasync latency (us)"
            );
            println!(
                "  kayactl ebpf block-latency  [--pid <pid>] [--run]        Trace block device I/O latency (us)"
            );
            println!(
                "  kayactl ebpf syscall-timeline [--pid <pid>] [--run]    Trace writes + fsync/rename etc + simple TID correlation (new)"
            );
            println!("  kayactl ebpf list                                    List local kayadb-server + active bpftrace processes (multi-node friendly)");
            println!("  kayactl ebpf status                                  Summary of nodes + attached traces");
            println!("  kayactl ebpf help");
            println!();
            println!("These commands print ready-to-run bpftrace invocations. Use --run [--duration 30s] to try executing directly (you will likely need sudo).");
            println!("--run improvements: best-effort duration limit via `timeout` (Unix), output shown to terminal; for clusters run multiple in separate terminals or use external timeout.");
            println!("Auto-detect + 'list'/'status' find all kayadb-server PIDs (good for 3-node local clusters).");
            println!("Tip for Track A: `kayactl --data ... flush` (or repeated puts) + `stats --latency` generates visible activity for the probes.");
            println!("They require a Linux host with bpftrace installed and sufficient capabilities (usually sudo).");
            println!();
            println!("Full documentation: scripts/ebpf/README.md");
            println!("Observability spec:   spec/docs/observability-spec.md (section 7)");
            println!("Roadmap:              ROADMAP.md (Track A)");
            if !cfg!(target_os = "linux") {
                println!("\n{}", LINUX_ONLY_MSG);
            } else if pid.is_none() {
                println!("\nTip: start a server first (kayadb-server or scripts/start-cluster.sh), then re-run. Use 'ebpf list' to discover PIDs.");
            }
            Ok(())
        }
    }
}

/// Try to exec the named bpftrace script (best-effort, for developer convenience).
/// On Linux + bpftrace present this will (optionally with duration) run the trace.
/// The user is responsible for privileges (the script will usually need sudo or
/// CAP_BPF+CAP_PERFMON). We deliberately do *not* auto-sudo here.
/// duration: optional e.g. "30s" — on Unix we prefix with `timeout <duration>` if the binary exists.
fn try_run_bpftrace(kind: &str, pid_str: &str, duration: Option<&str>) -> Result<()> {
    if !cfg!(target_os = "linux") {
        eprintln!("--run is only supported on Linux.");
        return Ok(());
    }

    let script = match kind {
        "fsync-latency" => "scripts/ebpf/fsync-latency.bt",
        "block-io-latency" => "scripts/ebpf/block-io-latency.bt",
        "syscall-timeline" => "scripts/ebpf/syscall-timeline.bt",
        _ => {
            eprintln!("unknown eBPF script kind");
            return Ok(());
        }
    };

    if pid_str == "<PID>" {
        eprintln!("Cannot --run without a concrete PID. Use --pid N or ensure a kayadb-server is running for auto-detect.");
        return Ok(());
    }

    // Check bpftrace exists
    if std::process::Command::new("bpftrace")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("bpftrace not found in PATH. Install it first (see scripts/ebpf/README.md).");
        return Ok(());
    }

    eprintln!("WARNING: launching bpftrace for PID {}.", pid_str);
    if let Some(d) = duration {
        eprintln!("Duration limited to {} (best effort via timeout).", d);
    }
    eprintln!("This may require elevated privileges. If it fails with permission errors, re-run the printed sudo command manually.");
    eprintln!("Press Ctrl-C to stop the trace.\n");

    let mut cmd = std::process::Command::new("bpftrace");
    if let Some(d) = duration {
        // Best effort: use timeout(1) when available (common on Linux). Fall back to plain run.
        if std::process::Command::new("timeout")
            .arg("--version")
            .output()
            .is_ok()
        {
            cmd.arg(d); // timeout <duration> bpftrace ...
        } else {
            eprintln!("Note: 'timeout' command not found; running without duration limit.");
        }
    }
    let status = cmd.args(["-p", pid_str, script]).status();

    match status {
        Ok(s) => {
            if !s.success() {
                eprintln!("bpftrace exited with status: {}", s);
            }
        }
        Err(e) => {
            eprintln!("Failed to spawn bpftrace: {}", e);
        }
    }
    Ok(())
}
