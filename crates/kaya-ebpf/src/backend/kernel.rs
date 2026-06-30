use crate::event::{ProbeEvent, SyscallKind};

/// Wire format emitted by `bpf/fsync_latency.bpf.c` ring buffer entries.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawFsyncEvent {
    pub latency_us: u64,
    pub syscall_kind: u8,
}

/// Parse a ring-buffer item into a probe event (shared by live drain + tests).
pub fn parse_raw_fsync_event(raw: &RawFsyncEvent, seq: u64) -> ProbeEvent {
    let syscall = if raw.syscall_kind == 1 {
        SyscallKind::Fdatasync
    } else {
        SyscallKind::Fsync
    };
    ProbeEvent::FsyncLatency {
        seq,
        syscall,
        latency_us: raw.latency_us.max(1),
        ts_ns: 0,
    }
}

/// Linux kernel eBPF probe loader (aya kprobe/kretprobe + ring buffer).
#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
pub struct KernelBackend {
    attached: bool,
    _bpf: aya::Ebpf,
    ring_buf: aya::maps::RingBuf<aya::maps::MapData>,
    pending: Vec<ProbeEvent>,
    next_seq: u64,
}

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
impl KernelBackend {
    pub fn try_attach() -> Result<Self, String> {
        #[cfg(kaya_ebpf_bpf_built)]
        {
            return Self::attach_from_object(include_bytes!(concat!(
                env!("OUT_DIR"),
                "/fsync_latency.bpf.o"
            )));
        }
        #[cfg(not(kaya_ebpf_bpf_built))]
        {
            Err(
                "kernel bpf object not built; install clang/llvm on Linux and rebuild with --features kernel-probes"
                    .into(),
            )
        }
    }

    /// Load and attach kprobes from a compiled BPF object (used by try_attach + tests).
    pub fn attach_from_object(bytes: &[u8]) -> Result<Self, String> {
        use aya::maps::RingBuf;
        use aya::programs::{KProbe, KRetprobe};
        use aya::Ebpf;

        let mut bpf = Ebpf::load(bytes).map_err(|e| format!("bpf load: {e}"))?;
        verify_programs_present(&bpf)?;
        attach_kprobe_pair(&mut bpf, "fsync_enter", "fsync_exit", "__x64_sys_fsync")?;
        attach_kprobe_pair(
            &mut bpf,
            "fdatasync_enter",
            "fdatasync_exit",
            "__x64_sys_fdatasync",
        )?;

        let ring_buf = RingBuf::try_from(bpf.take_map("events").ok_or("missing events map")?)
            .map_err(|e| format!("ringbuf: {e}"))?;

        Ok(Self {
            attached: true,
            _bpf: bpf,
            ring_buf,
            pending: Vec::new(),
            next_seq: 1,
        })
    }

    /// Load BPF object without attaching (CAP_BPF-free verification).
    pub fn verify_object_loads(bytes: &[u8]) -> Result<(), String> {
        use aya::Ebpf;
        let bpf = Ebpf::load(bytes).map_err(|e| format!("bpf load: {e}"))?;
        verify_programs_present(&bpf)
    }

    pub fn is_attached(&self) -> bool {
        self.attached
    }

    pub fn is_streaming(&self) -> bool {
        self.attached
    }

    pub fn detach(&mut self) -> bool {
        let was = self.attached;
        self.attached = false;
        was
    }

    pub fn drain_events(&mut self) -> Vec<ProbeEvent> {
        if !self.attached {
            return Vec::new();
        }
        while let Some(item) = self.ring_buf.next() {
            if item.len() < std::mem::size_of::<RawFsyncEvent>() {
                continue;
            }
            let raw = unsafe { *(item.as_ptr() as *const RawFsyncEvent) };
            self.pending.push(parse_raw_fsync_event(&raw, self.next_seq));
            self.next_seq += 1;
        }
        self.pending.drain(..).collect()
    }

}

/// Convert a batch of raw ring-buffer records into ordered probe events (test + replay).
pub fn parse_ringbuf_batch(items: &[RawFsyncEvent], start_seq: u64) -> Vec<ProbeEvent> {
    items
        .iter()
        .enumerate()
        .map(|(idx, raw)| parse_raw_fsync_event(raw, start_seq + idx as u64))
        .collect()
}

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
fn verify_programs_present(bpf: &aya::Ebpf) -> Result<(), String> {
    for name in [
        "fsync_enter",
        "fsync_exit",
        "fdatasync_enter",
        "fdatasync_exit",
    ] {
        if bpf.program(name).is_none() {
            return Err(format!("missing bpf program {name}"));
        }
    }
    if bpf.map("events").is_none() {
        return Err("missing bpf map events".into());
    }
    if bpf.map("start_ns").is_none() {
        return Err("missing bpf map start_ns".into());
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
fn attach_kprobe_pair(
    bpf: &mut aya::Ebpf,
    enter: &str,
    exit: &str,
    symbol: &str,
) -> Result<(), String> {
    use aya::programs::{KProbe, KRetprobe};

    let enter_prog: &mut KProbe = bpf
        .program_mut(enter)
        .ok_or_else(|| format!("missing {enter}"))?
        .try_into()
        .map_err(|e| format!("{enter} type: {e}"))?;
    enter_prog
        .load()
        .map_err(|e| format!("{enter} load: {e}"))?;
    enter_prog
        .attach(symbol, 0)
        .map_err(|e| format!("{enter} attach: {e}"))?;

    let exit_prog: &mut KRetprobe = bpf
        .program_mut(exit)
        .ok_or_else(|| format!("missing {exit}"))?
        .try_into()
        .map_err(|e| format!("{exit} type: {e}"))?;
    exit_prog
        .load()
        .map_err(|e| format!("{exit} load: {e}"))?;
    exit_prog
        .attach(symbol, 0)
        .map_err(|e| format!("{exit} attach: {e}"))?;
    Ok(())
}

/// Stub when kernel probes are not compiled for this target.
#[cfg(not(all(target_os = "linux", feature = "kernel-probes")))]
#[allow(dead_code)]
pub struct KernelBackend;

#[cfg(not(all(target_os = "linux", feature = "kernel-probes")))]
#[allow(dead_code)]
impl KernelBackend {
    pub fn try_attach() -> Result<Self, String> {
        Err("kernel probes require linux + kernel-probes feature".into())
    }

    pub fn is_attached(&self) -> bool {
        false
    }

    pub fn is_streaming(&self) -> bool {
        false
    }

    pub fn detach(&mut self) -> bool {
        false
    }

    pub fn drain_events(&mut self) -> Vec<ProbeEvent> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ringbuf_batch_preserves_order_and_sequence() {
        let events = parse_ringbuf_batch(
            &[
                RawFsyncEvent {
                    latency_us: 10,
                    syscall_kind: 0,
                },
                RawFsyncEvent {
                    latency_us: 20,
                    syscall_kind: 1,
                },
            ],
            5,
        );
        assert_eq!(events.len(), 2);
        let ProbeEvent::FsyncLatency { seq, .. } = &events[0];
        assert_eq!(*seq, 5);
        let ProbeEvent::FsyncLatency { seq, .. } = &events[1];
        assert_eq!(*seq, 6);
    }

    #[test]
    fn parse_raw_fsync_event_maps_syscall_kinds() {
        let fsync = parse_raw_fsync_event(
            &RawFsyncEvent {
                latency_us: 120,
                syscall_kind: 0,
            },
            1,
        );
        let fdatasync = parse_raw_fsync_event(
            &RawFsyncEvent {
                latency_us: 80,
                syscall_kind: 1,
            },
            2,
        );
        match fsync {
            ProbeEvent::FsyncLatency { syscall, latency_us, .. } => {
                assert_eq!(syscall, SyscallKind::Fsync);
                assert_eq!(latency_us, 120);
            }
        }
        match fdatasync {
            ProbeEvent::FsyncLatency { syscall, latency_us, .. } => {
                assert_eq!(syscall, SyscallKind::Fdatasync);
                assert_eq!(latency_us, 80);
            }
        }
    }

    #[test]
    fn kernel_attach_unavailable_without_linux_feature_combo() {
        assert!(KernelBackend::try_attach().is_err());
    }

    #[cfg(all(target_os = "linux", feature = "kernel-probes"))]
    mod linux_kernel {
        use super::*;

        #[cfg(kaya_ebpf_bpf_built)]
        #[test]
        fn bpf_object_loads_and_contains_programs() {
            let bytes = include_bytes!(concat!(env!("OUT_DIR"), "/fsync_latency.bpf.o"));
            KernelBackend::verify_object_loads(bytes).expect("bpf object must load");
        }

    }
}