#[cfg(target_os = "linux")]
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};
#[cfg(target_os = "linux")]
use std::thread;
#[cfg(target_os = "linux")]
use std::time::{Duration, Instant};

use kaya_core::{KayaError, Result};

/// Map catalog name to bpftrace script filename.
pub fn script_filename(name: &str) -> Result<&'static str> {
    match name {
        "fsync-latency" => Ok("fsync-latency.bt"),
        "block-io-latency" => Ok("block-io-latency.bt"),
        "block-latency" => Ok("block-io-latency.bt"),
        "syscall-timeline" => Ok("syscall-timeline.bt"),
        "durability-syscalls" => Ok("durability-syscalls.bt"),
        _ => Err(KayaError::invalid_argument(format!(
            "unknown ebpf script: {name}"
        ))),
    }
}

/// Resolve a catalog script name to an on-disk `.bt` path.
pub fn resolve_script(name: &str) -> Result<PathBuf> {
    let filename = script_filename(name)?;
    for dir in script_search_dirs() {
        let path = dir.join(filename);
        if path.is_file() {
            return Ok(path);
        }
    }
    Err(KayaError::invalid_argument(format!(
        "bpftrace script not found: {name} (expected {filename})"
    )))
}

/// Discover `kayadb-server` PIDs via `pgrep` (Linux only).
pub fn discover_server_pids() -> Vec<u32> {
    pgrep("-f", "kayadb-server")
}

/// Return `(pid, cmdline)` for each discovered `kayadb-server` process (Linux only).
pub fn server_pid_details() -> Vec<(u32, String)> {
    discover_server_pids()
        .into_iter()
        .map(|pid| (pid, pid_cmdline(pid)))
        .collect()
}

/// List active `bpftrace` PIDs via `pgrep` (Linux only).
pub fn list_active_bpftrace() -> Vec<u32> {
    pgrep("-f", "bpftrace")
}

const LINUX_ONLY_MSG: &str =
    "In-process eBPF probes are Linux-only. Use bpftrace scripts in scripts/ebpf/ on any Linux host.";

/// argv tail for `bpftrace -p <PID> <script>` (testable without spawning).
#[cfg(any(test, target_os = "linux"))]
pub fn bpftrace_command_args(pid: u32, script_path: &Path) -> Vec<String> {
    vec![
        "-p".to_owned(),
        pid.to_string(),
        script_path.display().to_string(),
    ]
}

/// Whether `bpftrace` is on `PATH` and responds to `--version` (Linux only).
#[cfg(target_os = "linux")]
fn bpftrace_available() -> bool {
    Command::new("bpftrace")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(target_os = "linux")]
fn bpftrace_missing_error() -> KayaError {
    eprintln!("bpftrace not found in PATH.");
    eprintln!("Install on Debian/Ubuntu: sudo apt install bpftrace");
    KayaError::NotFound
}

fn resolve_target_pid(pid: Option<u32>) -> Result<u32> {
    if let Some(pid) = pid {
        return Ok(pid);
    }
    discover_server_pids().first().copied().ok_or_else(|| {
        KayaError::invalid_argument(
            "no kayadb-server PID found; pass --pid or start kayadb-server first",
        )
    })
}

fn print_manual_bpftrace_instructions(pid: u32, script_path: &Path) {
    println!("Run bpftrace manually (Linux, typically requires sudo):");
    println!("  sudo bpftrace -p {pid} {}", script_path.display());
    println!();
    println!("Discover PIDs: kayactl ebpf list");
}

/// Resolve script, pick PID, then print manual instructions or spawn bpftrace.
#[cfg_attr(not(target_os = "linux"), allow(unused_variables))]
pub fn run_bpftrace_script(
    pid: Option<u32>,
    script_name: &str,
    run_mode: bool,
    duration_secs: u64,
) -> Result<()> {
    let script_path = resolve_script(script_name)?;

    if !cfg!(target_os = "linux") {
        println!("{LINUX_ONLY_MSG}");
        println!("Script: {}", script_path.display());
        if let Some(target_pid) = pid.or_else(|| discover_server_pids().first().copied()) {
            print_manual_bpftrace_instructions(target_pid, &script_path);
        } else {
            println!("Run bpftrace manually (Linux, typically requires sudo):");
            println!("  sudo bpftrace -p <PID> {}", script_path.display());
            println!();
            println!("Discover PIDs: kayactl ebpf list");
        }
        return Ok(());
    }

    let target_pid = resolve_target_pid(pid)?;

    if !run_mode {
        print_manual_bpftrace_instructions(target_pid, &script_path);
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        if !bpftrace_available() {
            return Err(bpftrace_missing_error());
        }
        return run_bpftrace_child(target_pid, &script_path, duration_secs);
    }

    #[cfg(not(target_os = "linux"))]
    unreachable!("linux-only run_mode branch reached on non-Linux")
}

#[cfg(target_os = "linux")]
fn run_bpftrace_child(pid: u32, script_path: &Path, duration_secs: u64) -> Result<()> {
    let args = bpftrace_command_args(pid, script_path);
    let mut child = Command::new("bpftrace")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| KayaError::Io {
            message: format!("failed to spawn bpftrace: {e}"),
        })?;

    if let Some(stdout) = child.stdout.take() {
        thread::spawn(move || stream_lines(stdout, false));
    }
    if let Some(stderr) = child.stderr.take() {
        thread::spawn(move || stream_lines(stderr, true));
    }

    let deadline = Instant::now() + Duration::from_secs(duration_secs.max(1));
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                return Err(KayaError::internal(format!(
                    "bpftrace exited with status {status}"
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let child_pid = child.id() as libc::pid_t;
                    unsafe {
                        libc::kill(child_pid, libc::SIGTERM);
                    }
                    let _ = child.wait();
                    println!(
                        "bpftrace stopped after {duration_secs}s (--duration timeout, SIGTERM sent)"
                    );
                    return Ok(());
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(e) => {
                return Err(KayaError::Io {
                    message: format!("waiting on bpftrace: {e}"),
                });
            }
        }
    }
}

#[cfg(target_os = "linux")]
fn stream_lines<R: std::io::Read + Send + 'static>(reader: R, is_stderr: bool) {
    let reader = BufReader::new(reader);
    for line in reader.lines() {
        match line {
            Ok(line) if is_stderr => eprintln!("{line}"),
            Ok(line) => println!("{line}"),
            Err(_) => break,
        }
    }
}

const CATALOG_SCRIPT_NAMES: &[&str] = &[
    "fsync-latency",
    "block-io-latency",
    "syscall-timeline",
    "durability-syscalls",
];

/// Comma-separated catalog script names from `kaya_ebpf::probe_catalog()` (static fallback on non-Linux).
pub fn format_catalog_script_names() -> String {
    let catalog = kaya_ebpf::probe_catalog();
    if catalog.is_empty() {
        return format_probe_names(CATALOG_SCRIPT_NAMES.iter().copied());
    }
    format_probe_names(catalog.iter().map(|p| p.name))
}

/// Join probe/script names for display (e.g. `kayactl ebpf list` catalog line).
pub fn format_probe_names<'a, I>(names: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    names.into_iter().collect::<Vec<_>>().join(", ")
}

#[cfg(target_os = "linux")]
fn pid_cmdline(pid: u32) -> String {
    if let Ok(output) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "args="])
        .output()
    {
        let args = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !args.is_empty() {
            return args;
        }
    }

    let path = format!("/proc/{pid}/cmdline");
    if let Ok(raw) = std::fs::read(&path) {
        let cmd = raw
            .split(|byte| byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| std::str::from_utf8(part).unwrap_or("?"))
            .collect::<Vec<_>>()
            .join(" ");
        if !cmd.is_empty() {
            return cmd;
        }
    }

    "(unknown)".to_owned()
}

#[cfg(not(target_os = "linux"))]
fn pid_cmdline(_pid: u32) -> String {
    String::new()
}

#[cfg(target_os = "linux")]
fn pgrep(flag: &str, pattern: &str) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .args([flag, pattern])
        .output()
        .ok()
        .map(|o| {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter_map(|line| line.trim().parse::<u32>().ok())
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn pgrep(_flag: &str, _pattern: &str) -> Vec<u32> {
    Vec::new()
}

fn normalize_script_dir(dir: &Path) -> PathBuf {
    if dir.join("fsync-latency.bt").is_file() {
        return dir.to_path_buf();
    }
    let nested = dir.join("scripts").join("ebpf");
    if nested.is_dir() {
        return nested;
    }
    dir.to_path_buf()
}

fn walk_up_for_scripts_ebpf(start: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut current = Some(start);
    while let Some(dir) = current {
        let candidate = dir.join("scripts").join("ebpf");
        if candidate.is_dir() {
            found.push(candidate);
        }
        current = dir.parent();
    }
    found
}

fn script_search_dirs() -> Vec<PathBuf> {
    let mut dirs = Vec::new();

    if let Ok(env_dir) = std::env::var("KAYA_EBPF_SCRIPT_DIR") {
        dirs.push(normalize_script_dir(Path::new(&env_dir)));
    }

    if let Ok(cwd) = std::env::current_dir() {
        dirs.push(cwd.join("scripts").join("ebpf"));
        dirs.extend(walk_up_for_scripts_ebpf(&cwd));
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(parent) = exe.parent() {
            dirs.extend(walk_up_for_scripts_ebpf(parent));
        }
    }

    let mut seen = Vec::new();
    for dir in dirs {
        if seen.iter().any(|existing: &PathBuf| existing == &dir) {
            continue;
        }
        seen.push(dir);
    }
    seen
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    #[test]
    fn script_filename_maps_block_latency_alias() {
        assert_eq!(
            script_filename("block-latency").unwrap(),
            "block-io-latency.bt"
        );
    }

    #[test]
    fn format_probe_names_joins_catalog_entries() {
        let names = ["fsync-latency", "block-io-latency", "syscall-timeline"];
        assert_eq!(
            format_probe_names(names),
            "fsync-latency, block-io-latency, syscall-timeline"
        );
    }

    #[test]
    fn format_catalog_script_names_matches_probe_catalog_or_static_fallback() {
        let catalog = kaya_ebpf::probe_catalog();
        let expected = if catalog.is_empty() {
            format_probe_names(CATALOG_SCRIPT_NAMES.iter().copied())
        } else {
            format_probe_names(catalog.iter().map(|p| p.name))
        };
        assert_eq!(format_catalog_script_names(), expected);
    }

    #[test]
    fn bpftrace_command_args_includes_pid_and_script_path() {
        let script = Path::new("/repo/scripts/ebpf/fsync-latency.bt");
        assert_eq!(
            bpftrace_command_args(4242, script),
            vec![
                "-p".to_owned(),
                "4242".to_owned(),
                "/repo/scripts/ebpf/fsync-latency.bt".to_owned(),
            ]
        );
    }

    #[test]
    fn bpftrace_command_args_for_block_latency_alias_script() {
        let root = repo_root();
        let prev = std::env::current_dir().ok();
        std::env::set_current_dir(&root).expect("set cwd to repo root");
        let script_path = resolve_script("block-latency").expect("block-io-latency.bt");
        if let Some(dir) = prev {
            let _ = std::env::set_current_dir(dir);
        }

        let args = bpftrace_command_args(99, &script_path);
        assert_eq!(args[0], "-p");
        assert_eq!(args[1], "99");
        assert!(args[2]
            .replace('\\', "/")
            .ends_with("scripts/ebpf/block-io-latency.bt"));
    }

    #[test]
    fn resolve_fsync_latency_script_from_cwd() {
        let root = repo_root();
        let prev = std::env::current_dir().ok();
        std::env::set_current_dir(&root).expect("set cwd to repo root");
        let result = resolve_script("fsync-latency");
        if let Some(dir) = prev {
            let _ = std::env::set_current_dir(dir);
        }

        let path = result.expect("fsync-latency.bt should resolve from repo root cwd");
        let normalized = path.to_string_lossy().replace('\\', "/");
        assert!(normalized.ends_with("scripts/ebpf/fsync-latency.bt"));
    }
}
