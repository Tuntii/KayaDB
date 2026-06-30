use crate::event::ProbeEvent;
use crate::histogram::FsyncHistogram;

/// Pure ingest → histogram → ordered event store (no backend I/O).
#[derive(Debug, Clone)]
pub struct EventPipeline {
    histogram: FsyncHistogram,
    collected: Vec<ProbeEvent>,
}

impl Default for EventPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl EventPipeline {
    pub fn new() -> Self {
        Self {
            histogram: FsyncHistogram::new(),
            collected: Vec::new(),
        }
    }

    pub fn histogram(&self) -> &FsyncHistogram {
        &self.histogram
    }

    pub fn events(&self) -> &[ProbeEvent] {
        &self.collected
    }

    pub fn event_count(&self) -> u64 {
        self.collected.len() as u64
    }

    pub fn ingest_batch(&mut self, mut drained: Vec<ProbeEvent>) {
        for mut event in drained.drain(..) {
            let seq = self.collected.len() as u64 + 1;
            let ProbeEvent::FsyncLatency { seq: ref mut event_seq, .. } = &mut event;
            *event_seq = seq;
            self.histogram.ingest(&event);
            self.collected.push(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SyscallKind;

    #[test]
    fn ingest_assigns_monotonic_sequence_and_histogram() {
        let mut pipe = EventPipeline::new();
        pipe.ingest_batch(vec![
            ProbeEvent::FsyncLatency {
                seq: 0,
                syscall: SyscallKind::Fsync,
                latency_us: 120,
                ts_ns: 1,
            },
            ProbeEvent::FsyncLatency {
                seq: 0,
                syscall: SyscallKind::Fdatasync,
                latency_us: 80,
                ts_ns: 2,
            },
        ]);
        assert_eq!(pipe.event_count(), 2);
        assert_eq!(pipe.histogram().total_count(), 2);
        assert_eq!(
            pipe.events()[1],
            ProbeEvent::FsyncLatency {
                seq: 2,
                syscall: SyscallKind::Fdatasync,
                latency_us: 80,
                ts_ns: 2,
            }
        );
    }
}