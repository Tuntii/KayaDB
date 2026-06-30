//! Kernel ring-buffer parsing and BPF object verification tests.

use kaya_ebpf::backend::kernel::{
    parse_raw_fsync_event_at, parse_ringbuf_batch, RawFsyncEvent,
};
use kaya_ebpf::{ProbeEvent, SyscallKind};

#[test]
fn bpf_source_declares_target_pid_filter() {
    let bpf_src = include_str!("../bpf/fsync_latency.bpf.c");
    assert!(bpf_src.contains("target_pid"), "bpf must declare target_pid map");
    assert!(bpf_src.contains("pid_allowed"), "bpf must filter by target pid");

    let rust_src = include_str!("../src/backend/kernel.rs");
    assert!(
        rust_src.contains("set_target_pid_map"),
        "live attach must write target_pid map from userspace"
    );
    assert!(
        rust_src.contains("parse_raw_fsync_event_at"),
        "live ringbuf drain must stamp ts_ns at userspace drain time"
    );
}

#[test]
fn live_ringbuf_parse_stamps_nonzero_ts_ns() {
    let raw = RawFsyncEvent {
        latency_us: 512,
        syscall_kind: 0,
    };
    let event = parse_raw_fsync_event_at(&raw, 1, 9_876_543_210);
    match event {
        ProbeEvent::FsyncLatency {
            ts_ns,
            latency_us,
            syscall,
            ..
        } => {
            assert_eq!(ts_ns, 9_876_543_210);
            assert_eq!(latency_us, 512);
            assert_eq!(syscall, SyscallKind::Fsync);
        }
    }
}

#[test]
fn ringbuf_batch_produces_kernel_shaped_probe_events() {
    let events = parse_ringbuf_batch(
        &[
            RawFsyncEvent {
                latency_us: 333,
                syscall_kind: 0,
            },
            RawFsyncEvent {
                latency_us: 77,
                syscall_kind: 1,
            },
        ],
        1,
    );
    assert_eq!(events.len(), 2);
    assert!(matches!(
        events[0],
        ProbeEvent::FsyncLatency {
            syscall: SyscallKind::Fsync,
            latency_us: 333,
            ..
        }
    ));
    assert!(matches!(
        events[1],
        ProbeEvent::FsyncLatency {
            syscall: SyscallKind::Fdatasync,
            latency_us: 77,
            ..
        }
    ));
}

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
mod linux {
    use kaya_ebpf::backend::kernel::KernelBackend;

    #[cfg(kaya_ebpf_bpf_built)]
    #[test]
    fn bpf_object_loads_without_cap_bpf() {
        let bytes = include_bytes!(concat!(env!("OUT_DIR"), "/fsync_latency.bpf.o"));
        KernelBackend::verify_object_loads(bytes).expect("compiled bpf object must load via aya");
    }

    #[cfg(kaya_ebpf_bpf_built)]
    #[test]
    #[ignore = "requires CAP_BPF; run: KAYA_EBPF_LIVE_KERNEL=1 cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach -- --ignored"]
    fn live_kernel_attach_streams_events() {
        if std::env::var("KAYA_EBPF_LIVE_KERNEL").ok().as_deref() != Some("1") {
            panic!("set KAYA_EBPF_LIVE_KERNEL=1 to run live kernel attach test");
        }
        let mut backend =
            KernelBackend::try_attach().expect("live kernel attach requires CAP_BPF");
        assert!(backend.is_streaming());
        let file = tempfile::NamedTempFile::new().expect("temp file");
        file.as_file().sync_all().expect("fsync");
        std::thread::sleep(std::time::Duration::from_millis(100));
        let events = backend.drain_events();
        assert!(
            !events.is_empty(),
            "expected kernel ringbuf events after fsync syscall"
        );
        backend.detach();
    }
}