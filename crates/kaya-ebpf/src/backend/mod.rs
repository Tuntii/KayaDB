mod simulated;
mod tap;

pub use simulated::SimulatedBackend;
pub use tap::TapBackend;

use crate::event::ProbeEvent;

/// Event source feeding the probe manager pipeline.
pub trait EventBackend: Send {
    fn attach(&mut self) -> Result<(), String>;
    fn detach(&mut self) -> bool;
    fn is_attached(&self) -> bool;
    fn backend_name(&self) -> &'static str;
    fn drain_events(&mut self) -> Vec<ProbeEvent>;
}

/// Composite backend used on Linux: userspace tap plus optional simulated seed stream.
#[cfg(target_os = "linux")]
pub struct LinuxCompositeBackend {
    tap: TapBackend,
    simulated: Option<SimulatedBackend>,
}

#[cfg(target_os = "linux")]
impl LinuxCompositeBackend {
    pub fn new(seed: Option<u64>) -> Self {
        Self {
            tap: TapBackend::new(),
            simulated: seed.map(SimulatedBackend::new),
        }
    }
}

#[cfg(target_os = "linux")]
impl LinuxCompositeBackend {
    pub fn report_fsync(&mut self, syscall: crate::event::SyscallKind, latency_us: u64, ts_ns: u64) {
        self.tap.report_fsync(syscall, latency_us, ts_ns);
    }
}

#[cfg(target_os = "linux")]
impl EventBackend for LinuxCompositeBackend {
    fn attach(&mut self) -> Result<(), String> {
        self.tap.attach()?;
        if let Some(sim) = &mut self.simulated {
            sim.attach()?;
        }
        Ok(())
    }

    fn detach(&mut self) -> bool {
        let mut detached = self.tap.detach();
        if let Some(sim) = &mut self.simulated {
            detached &= sim.detach();
        }
        detached
    }

    fn is_attached(&self) -> bool {
        self.tap.is_attached()
    }

    fn backend_name(&self) -> &'static str {
        if self.simulated.is_some() {
            "linux-tap+simulated"
        } else {
            "linux-userspace-tap"
        }
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        let mut out = self.tap.drain_events();
        if let Some(sim) = &mut self.simulated {
            out.extend(sim.drain_events());
            out.sort_by_key(|e| e.seq());
        }
        out
    }
}

/// Non-Linux no-op backend that only streams seeded simulated events in tests.
pub struct StubBackend {
    simulated: Option<SimulatedBackend>,
}

impl StubBackend {
    pub fn new(seed: Option<u64>) -> Self {
        Self {
            simulated: seed.map(SimulatedBackend::new),
        }
    }
}

impl EventBackend for StubBackend {
    fn attach(&mut self) -> Result<(), String> {
        if let Some(sim) = &mut self.simulated {
            sim.attach()?;
        }
        Ok(())
    }

    fn detach(&mut self) -> bool {
        self.simulated
            .as_mut()
            .map(|s| s.detach())
            .unwrap_or(true)
    }

    fn is_attached(&self) -> bool {
        self.simulated.as_ref().is_some_and(|s| s.is_attached())
    }

    fn backend_name(&self) -> &'static str {
        "noop-stub"
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        self.simulated
            .as_mut()
            .map(|s| s.drain_events())
            .unwrap_or_default()
    }
}