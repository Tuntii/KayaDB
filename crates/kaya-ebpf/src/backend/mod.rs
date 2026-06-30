mod kernel;
mod simulated;
mod tap;

#[cfg(target_os = "linux")]
pub use kernel::KernelBackend;
pub use simulated::SimulatedBackend;
pub use tap::TapBackend;

use crate::event::{ProbeEvent, SyscallKind};

/// Event source feeding the probe manager pipeline.
pub trait EventBackend: Send {
    fn attach(&mut self) -> Result<(), String>;
    fn detach(&mut self) -> bool;
    fn is_attached(&self) -> bool;
    fn backend_name(&self) -> &'static str;
    fn drain_events(&mut self) -> Vec<ProbeEvent>;
    fn report_fsync(&mut self, _syscall: SyscallKind, _latency_us: u64, _ts_ns: u64) {}
}

/// Composite backend on Linux: kernel probes (when available) + userspace tap + optional sim.
#[cfg(target_os = "linux")]
pub struct LinuxCompositeBackend {
    tap: TapBackend,
    kernel: Option<KernelBackend>,
    simulated: Option<SimulatedBackend>,
    kernel_attempted: bool,
}

#[cfg(target_os = "linux")]
impl LinuxCompositeBackend {
    pub fn new(seed: Option<u64>, try_kernel: bool) -> Self {
        let mut kernel = None;
        let mut kernel_attempted = false;
        if try_kernel {
            kernel_attempted = true;
            kernel = KernelBackend::try_attach().ok();
        }
        Self {
            tap: TapBackend::new(),
            kernel,
            simulated: seed.map(SimulatedBackend::new),
            kernel_attempted,
        }
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
        if self.kernel.as_ref().is_some_and(|k| k.is_attached()) {
            "linux-kernel+tap"
        } else if self.kernel_attempted {
            "linux-tap-kernel-fallback"
        } else if self.simulated.is_some() {
            "linux-tap+simulated"
        } else {
            "linux-userspace-tap"
        }
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        let mut out = Vec::new();
        if let Some(kernel) = &mut self.kernel {
            out.extend(kernel.drain_events());
        }
        out.extend(self.tap.drain_events());
        if let Some(sim) = &mut self.simulated {
            out.extend(sim.drain_events());
        }
        out
    }

    fn report_fsync(&mut self, syscall: SyscallKind, latency_us: u64, ts_ns: u64) {
        self.tap.report_fsync(syscall, latency_us, ts_ns);
    }
}

/// Non-Linux no-op backend: userspace tap only (no simulated events unless seeded in tests).
pub struct StubBackend {
    tap: TapBackend,
    simulated: Option<SimulatedBackend>,
}

impl StubBackend {
    pub fn new(seed: Option<u64>) -> Self {
        Self {
            tap: TapBackend::new(),
            simulated: seed.map(SimulatedBackend::new),
        }
    }
}

impl EventBackend for StubBackend {
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
            "noop-stub+simulated"
        } else {
            "noop-stub"
        }
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        let mut out = self.tap.drain_events();
        if let Some(sim) = &mut self.simulated {
            out.extend(sim.drain_events());
        }
        out
    }

    fn report_fsync(&mut self, syscall: SyscallKind, latency_us: u64, ts_ns: u64) {
        self.tap.report_fsync(syscall, latency_us, ts_ns);
    }
}