use crate::event::ProbeEvent;
#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
use crate::event::SyscallKind;

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
#[repr(C)]
struct RawFsyncEvent {
    latency_us: u64,
    syscall_kind: u8,
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
                "kernel bpf object not built; rebuild on Linux with clang and --features kernel-probes"
                    .into(),
            )
        }
    }

    fn attach_from_object(bytes: &[u8]) -> Result<Self, String> {
        use aya::maps::RingBuf;
        use aya::programs::{KProbe, KRetprobe};
        use aya::Ebpf;

        let mut bpf = Ebpf::load(bytes).map_err(|e| format!("bpf load: {e}"))?;
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

    pub fn is_attached(&self) -> bool {
        self.attached
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
            let syscall = if raw.syscall_kind == 1 {
                SyscallKind::Fdatasync
            } else {
                SyscallKind::Fsync
            };
            self.pending.push(ProbeEvent::FsyncLatency {
                seq: self.next_seq,
                syscall,
                latency_us: raw.latency_us.max(1),
                ts_ns: 0,
            });
            self.next_seq += 1;
        }
        self.pending.drain(..).collect()
    }
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

    pub fn drain_events(&mut self) -> Vec<ProbeEvent> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_attach_unavailable_without_linux_feature_combo() {
        assert!(KernelBackend::try_attach().is_err());
    }
}