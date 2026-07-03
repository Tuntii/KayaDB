use std::path::{Path, PathBuf};

use kaya_core::{KayaError, Result};

/// Map catalog name to bpftrace script filename.
pub fn script_filename(name: &str) -> Result<&'static str> {
    match name {
        "fsync-latency" => Ok("fsync-latency.bt"),
        "block-io-latency" => Ok("block-io-latency.bt"),
        "block-latency" => Ok("block-io-latency.bt"),
        "syscall-timeline" => Ok("syscall-timeline.bt"),
        "durability-syscalls" => Ok("durability-syscalls.bt"),
        _ => Err(KayaError::invalid_argument(format!("unknown ebpf script: {name}"))),
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

/// List active `bpftrace` PIDs via `pgrep` (Linux only).
pub fn list_active_bpftrace() -> Vec<u32> {
    pgrep("-f", "bpftrace")
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