//! Optional Linux eBPF scaffolding (stub).
//!
//! Real tracing uses bpftrace scripts under `scripts/ebpf/`. This crate exists so
//! future in-process probes can live behind a non-hard workspace dependency.

/// Metadata for a bpftrace probe script shipped with KayaDB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeInfo {
    pub name: &'static str,
    pub script_path: &'static str,
}

/// Whether an eBPF probe is attached in-process (always false in the stub).
pub fn probe_attached() -> bool {
    false
}

#[cfg(target_os = "linux")]
pub mod linux {
    //! Linux-only catalog of bpftrace scripts under `scripts/ebpf/`.

    const PROBES: &[(&str, &str)] = &[
        ("fsync-latency", "scripts/ebpf/fsync-latency.bt"),
        ("block-io-latency", "scripts/ebpf/block-io-latency.bt"),
        ("syscall-timeline", "scripts/ebpf/syscall-timeline.bt"),
        ("durability-syscalls", "scripts/ebpf/durability-syscalls.bt"),
    ];

    /// Documented build prerequisites for real eBPF crates.
    pub const BUILD_NOTES: &str =
        "Requires clang, llvm, bpf-linker or libbpf; not needed for `cargo test --workspace`.";

    /// Relative paths to bpftrace scripts shipped in `scripts/ebpf/`.
    pub fn available_scripts() -> Vec<&'static str> {
        PROBES.iter().map(|(_, path)| *path).collect()
    }

    /// Catalog of named probes and their script paths.
    pub fn probe_catalog() -> Vec<super::ProbeInfo> {
        PROBES
            .iter()
            .map(|(name, path)| super::ProbeInfo {
                name,
                script_path: path,
            })
            .collect()
    }
}

#[cfg(not(target_os = "linux"))]
mod stub {
    pub fn available_scripts() -> Vec<&'static str> {
        Vec::new()
    }

    pub fn probe_catalog() -> Vec<super::ProbeInfo> {
        Vec::new()
    }
}

/// Relative paths to bpftrace scripts shipped in `scripts/ebpf/`.
///
/// Returns an empty list on non-Linux platforms.
pub fn available_scripts() -> Vec<&'static str> {
    inner_available_scripts()
}

/// Catalog of named probes and their script paths.
///
/// Returns an empty list on non-Linux platforms.
pub fn probe_catalog() -> Vec<ProbeInfo> {
    inner_probe_catalog()
}

#[cfg(target_os = "linux")]
fn inner_available_scripts() -> Vec<&'static str> {
    linux::available_scripts()
}

#[cfg(not(target_os = "linux"))]
fn inner_available_scripts() -> Vec<&'static str> {
    stub::available_scripts()
}

#[cfg(target_os = "linux")]
fn inner_probe_catalog() -> Vec<ProbeInfo> {
    linux::probe_catalog()
}

#[cfg(not(target_os = "linux"))]
fn inner_probe_catalog() -> Vec<ProbeInfo> {
    stub::probe_catalog()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_attached_is_false() {
        assert!(!probe_attached());
    }

    #[test]
    fn available_scripts_and_catalog_agree() {
        let scripts = available_scripts();
        let catalog = probe_catalog();
        let paths: Vec<_> = catalog.iter().map(|p| p.script_path).collect();
        assert_eq!(scripts, paths);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn linux_catalog_lists_known_probes() {
        let catalog = probe_catalog();
        assert_eq!(catalog.len(), 4);
        assert!(catalog.iter().any(|p| p.name == "fsync-latency"));
        assert!(catalog.iter().any(|p| p.name == "block-io-latency"));
        assert!(catalog.iter().any(|p| p.name == "syscall-timeline"));
        assert!(catalog.iter().any(|p| p.name == "durability-syscalls"));
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn non_linux_returns_empty() {
        assert!(available_scripts().is_empty());
        assert!(probe_catalog().is_empty());
    }
}