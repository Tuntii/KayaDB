use std::collections::VecDeque;

use crate::backend::EventBackend;
use crate::event::{ProbeEvent, SyscallKind};

/// Userspace tap that ingests real fsync latency samples from the server process.
pub struct TapBackend {
    attached: bool,
    next_seq: u64,
    pending: VecDeque<ProbeEvent>,
}

impl TapBackend {
    pub fn new() -> Self {
        Self {
            attached: false,
            next_seq: 1,
            pending: VecDeque::new(),
        }
    }

    pub fn report_fsync(&mut self, syscall: SyscallKind, latency_us: u64, ts_ns: u64) {
        if !self.attached {
            return;
        }
        self.pending.push_back(ProbeEvent::FsyncLatency {
            seq: self.next_seq,
            syscall,
            latency_us,
            ts_ns,
        });
        self.next_seq += 1;
    }

    pub fn report_from_engine_delta(
        &mut self,
        delta_total_us: u64,
        max_us: u64,
        ts_ns: u64,
    ) {
        if delta_total_us == 0 {
            return;
        }
        self.report_fsync(SyscallKind::Fsync, max_us.max(1), ts_ns);
        if delta_total_us > max_us {
            self.report_fsync(
                SyscallKind::Fdatasync,
                (delta_total_us - max_us).max(1),
                ts_ns.wrapping_add(1),
            );
        }
    }
}

impl Default for TapBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBackend for TapBackend {
    fn attach(&mut self) -> Result<(), String> {
        self.attached = true;
        Ok(())
    }

    fn detach(&mut self) -> bool {
        let was = self.attached;
        self.attached = false;
        was
    }

    fn is_attached(&self) -> bool {
        self.attached
    }

    fn backend_name(&self) -> &'static str {
        "userspace-tap"
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        self.pending.drain(..).collect()
    }
}