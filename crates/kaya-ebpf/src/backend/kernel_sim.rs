use std::collections::VecDeque;

use crate::backend::kernel::{parse_ringbuf_batch, RawFsyncEvent};
use crate::event::{ProbeEvent, SyscallKind};

/// Deterministic kernel-slot backend: ringbuf-shaped events without CAP_BPF.
pub struct KernelSimulatedBackend {
    seed: u64,
    attached: bool,
    pending: VecDeque<ProbeEvent>,
    boot_batch_emitted: bool,
}

impl KernelSimulatedBackend {
    pub fn new(seed: u64) -> Self {
        Self {
            seed,
            attached: false,
            pending: VecDeque::new(),
            boot_batch_emitted: false,
        }
    }

    fn emit_boot_batch(&mut self) {
        if self.boot_batch_emitted {
            return;
        }
        let raw = [
            RawFsyncEvent {
                latency_us: 80 + (self.seed % 40),
                syscall_kind: 0,
            },
            RawFsyncEvent {
                latency_us: 40 + (self.seed % 20),
                syscall_kind: 1,
            },
        ];
        for (i, mut event) in parse_ringbuf_batch(&raw, 1).into_iter().enumerate() {
            let ProbeEvent::FsyncLatency { ref mut ts_ns, .. } = &mut event;
            *ts_ns = self.seed.wrapping_mul(1_000).wrapping_add(i as u64 + 1);
            self.pending.push_back(event);
        }
        self.boot_batch_emitted = true;
    }

    /// Synthesize per-op kernel samples from observed WAL fsync activity.
    pub fn sync_wal_activity(&mut self, delta_total_us: u64, max_us: u64) {
        if !self.attached || delta_total_us == 0 {
            return;
        }
        let ts_base = self.seed.wrapping_mul(1_000).wrapping_add(self.pending.len() as u64);
        self.pending.push_back(ProbeEvent::FsyncLatency {
            seq: 0,
            syscall: SyscallKind::Fsync,
            latency_us: max_us.max(1),
            ts_ns: ts_base,
        });
        if delta_total_us > max_us {
            self.pending.push_back(ProbeEvent::FsyncLatency {
                seq: 0,
                syscall: SyscallKind::Fdatasync,
                latency_us: (delta_total_us - max_us).max(1),
                ts_ns: ts_base.wrapping_add(1),
            });
        }
    }
}

impl KernelSimulatedBackend {
    pub fn attach(&mut self) -> Result<(), String> {
        self.attached = true;
        self.emit_boot_batch();
        Ok(())
    }

    pub fn detach(&mut self) -> bool {
        let was = self.attached;
        self.attached = false;
        was
    }

    pub fn is_attached(&self) -> bool {
        self.attached
    }

    pub fn backend_name(&self) -> &'static str {
        "kernel-simulated"
    }

    pub fn kernel_streaming(&self) -> bool {
        self.attached
    }

    pub fn drain_events(&mut self) -> Vec<ProbeEvent> {
        if !self.attached {
            return Vec::new();
        }
        self.pending.drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_ringbuf_shaped_boot_batch_with_nonzero_ts() {
        let mut b = KernelSimulatedBackend::new(42);
        b.attach().unwrap();
        let events = b.drain_events();
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| matches!(e, ProbeEvent::FsyncLatency { ts_ns, .. } if *ts_ns > 0)));
        assert!(b.kernel_streaming());
        assert_eq!(b.backend_name(), "kernel-simulated");
    }

    #[test]
    fn sync_wal_activity_appends_kernel_shaped_events() {
        let mut b = KernelSimulatedBackend::new(7);
        b.attach().unwrap();
        let _ = b.drain_events();
        b.sync_wal_activity(500, 120);
        let events = b.drain_events();
        assert!(events.len() >= 1);
        assert!(events.iter().any(|e| matches!(
            e,
            ProbeEvent::FsyncLatency {
                syscall: SyscallKind::Fsync,
                latency_us: 120,
                ..
            }
        )));
    }
}