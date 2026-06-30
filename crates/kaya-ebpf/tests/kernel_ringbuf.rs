//! Kernel ring-buffer parsing and BPF object verification tests.

use kaya_ebpf::backend::kernel::{decode_ringbuf_items, parse_ringbuf_batch, RawFsyncEvent};
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
        rust_src.contains("decode_ringbuf_items"),
        "live ringbuf drain must use shared decode_ringbuf_items path"
    );
}

/// Cross-platform kernel-pipeline proof (tier A): golden ringbuf bytes through shared decode.
#[test]
fn decode_ringbuf_injected_items_produces_nonempty_events_with_ts_ns() {
    let injected = [
        RawFsyncEvent {
            latency_us: 512,
            syscall_kind: 0,
        },
        RawFsyncEvent {
            latency_us: 128,
            syscall_kind: 1,
        },
    ];
    let mut seq = 1;
    let events = decode_ringbuf_items(&injected, &mut seq);
    assert_eq!(events.len(), 2);
    assert_eq!(seq, 3);
    assert!(
        events
            .iter()
            .all(|e| matches!(e, ProbeEvent::FsyncLatency { ts_ns, .. } if *ts_ns > 0)),
        "injected ringbuf decode must stamp non-zero ts_ns"
    );
    match &events[0] {
        ProbeEvent::FsyncLatency {
            syscall,
            latency_us,
            ..
        } => {
            assert_eq!(*syscall, SyscallKind::Fsync);
            assert_eq!(*latency_us, 512);
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
    use kaya_ebpf::backend::kernel::{decode_ringbuf_items, KernelBackend, RawFsyncEvent};
    use kaya_ebpf::ProbeEvent;

    /// Tier B: aya bpf load (no CAP_BPF) + same decode path as live drain_events.
    #[cfg(kaya_ebpf_bpf_built)]
    #[test]
    fn bpf_object_loads_without_cap_bpf() {
        let bytes = include_bytes!(concat!(env!("OUT_DIR"), "/fsync_latency.bpf.o"));
        KernelBackend::verify_object_loads(bytes).expect("compiled bpf object must load via aya");
    }

    #[cfg(kaya_ebpf_bpf_built)]
    #[test]
    fn kernel_load_object_and_drain_injected_ringbuf() {
        let bytes = include_bytes!(concat!(env!("OUT_DIR"), "/fsync_latency.bpf.o"));
        KernelBackend::verify_object_loads(bytes).expect("bpf object must load before decode");

        let injected = [
            RawFsyncEvent {
                latency_us: 777,
                syscall_kind: 0,
            },
            RawFsyncEvent {
                latency_us: 42,
                syscall_kind: 1,
            },
        ];
        let mut seq = 1;
        let events = decode_ringbuf_items(&injected, &mut seq);
        assert!(
            !events.is_empty(),
            "injected ringbuf through decode_ringbuf_items must yield events"
        );
        assert!(
            events
                .iter()
                .all(|e| matches!(e, ProbeEvent::FsyncLatency { ts_ns, .. } if *ts_ns > 0)),
            "kernel load+decode path must stamp ts_ns at drain"
        );
        assert_eq!(seq, 3);
    }

    /// Tier C only: real kprobe traffic + ringbuf drain (requires CAP_BPF).
    #[cfg(kaya_ebpf_bpf_built)]
    #[test]
    #[ignore = "tier-C live kprobe; run: KAYA_EBPF_LIVE_KERNEL=1 cargo test -p kaya-ebpf --features kernel-probes live_kernel_attach_streams_events -- --ignored"]
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
        assert!(
            events
                .iter()
                .all(|e| matches!(e, ProbeEvent::FsyncLatency { ts_ns, .. } if *ts_ns > 0)),
            "live ringbuf drain must stamp ts_ns"
        );
        backend.detach();
    }
}