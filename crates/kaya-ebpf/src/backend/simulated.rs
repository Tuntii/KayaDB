use crate::backend::EventBackend;
use crate::event::ProbeEvent;
use crate::trace::seeded_fsync_events;

/// Deterministic seeded event generator for CI and non-Linux hosts.
pub struct SimulatedBackend {
    seed: u64,
    attached: bool,
    events: Vec<ProbeEvent>,
    cursor: usize,
}

impl SimulatedBackend {
    pub fn attach(&mut self) -> Result<(), String> {
        self.attached = true;
        self.cursor = 0;
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
        "simulated"
    }

    pub fn drain_events(&mut self) -> Vec<ProbeEvent> {
        if !self.attached {
            return Vec::new();
        }
        let mut out = Vec::new();
        while self.cursor < self.events.len() {
            out.push(self.events[self.cursor].clone());
            self.cursor += 1;
        }
        out
    }

    pub fn new(seed: u64) -> Self {
        let events = seeded_fsync_events(seed, 8);
        Self {
            seed,
            attached: false,
            events,
            cursor: 0,
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl EventBackend for SimulatedBackend {
    fn attach(&mut self) -> Result<(), String> {
        SimulatedBackend::attach(self)
    }
    fn detach(&mut self) -> bool {
        SimulatedBackend::detach(self)
    }
    fn is_attached(&self) -> bool {
        SimulatedBackend::is_attached(self)
    }
    fn backend_name(&self) -> &'static str {
        SimulatedBackend::backend_name(self)
    }
    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        SimulatedBackend::drain_events(self)
    }
}