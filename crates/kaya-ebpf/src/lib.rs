//! Optional Linux eBPF scaffolding (stub).
//!
//! Real tracing uses bpftrace scripts under `scripts/ebpf/`. This crate exists so
//! future in-process probes can live behind a non-hard workspace dependency.

/// Stub API version string.
pub const STUB_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Whether an eBPF probe is attached in-process (always false in the stub).
pub fn probe_attached() -> bool {
    false
}

#[cfg(target_os = "linux")]
pub mod linux {
    //! Linux-only placeholder for future Aya/libbpf probes.

    /// Documented build prerequisites for real eBPF crates.
    pub const BUILD_NOTES: &str =
        "Requires clang, llvm, bpf-linker or libbpf; not needed for `cargo test --workspace`.";

    /// Syscall groups traced by `scripts/ebpf/durability-syscalls.bt`.
    pub const DURABILITY_SYSCALLS: &[&str] = &[
        "write",
        "writev",
        "pwrite64",
        "fsync",
        "fdatasync",
        "rename",
        "unlink",
        "fsyncdir",
    ];
}
