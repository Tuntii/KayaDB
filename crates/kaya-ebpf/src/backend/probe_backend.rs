use crate::backend::kernel_sim::KernelSimulatedBackend;
use crate::backend::simulated::SimulatedBackend;
use crate::backend::tap::TapBackend;
use crate::event::{ProbeEvent, SyscallKind};
#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
use crate::backend::kernel::KernelBackend;

/// Explicit probe backend slot — no silent kernel+tap mixing.
#[allow(clippy::large_enum_variant)]
pub enum ProbeBackend {
    KernelLive(KernelLiveSlot),
    KernelSimulated(KernelSimulatedBackend),
    Tap(TapBackend),
    TestSimulated(SimulatedBackend),
    Noop,
}

/// Live kernel backend attached during `attach()`, not at construction.
#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
pub struct KernelLiveSlot {
    inner: Option<KernelBackend>,
    attach_error: Option<String>,
}

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
impl KernelLiveSlot {
    fn new_deferred() -> Self {
        Self {
            inner: None,
            attach_error: None,
        }
    }

    fn live(&self) -> Option<&KernelBackend> {
        self.inner.as_ref()
    }

    fn live_mut(&mut self) -> Option<&mut KernelBackend> {
        self.inner.as_mut()
    }
}

#[cfg(not(all(target_os = "linux", feature = "kernel-probes")))]
pub struct KernelLiveSlot;

impl ProbeBackend {
    pub fn build(selection: BackendSelection, seed: u64) -> Self {
        match selection {
            BackendSelection::KernelPreferred => Self::build_kernel_preferred(seed),
            BackendSelection::KernelSimulated => {
                Self::KernelSimulated(KernelSimulatedBackend::new(seed))
            }
            BackendSelection::Tap => Self::Tap(TapBackend::new()),
            BackendSelection::TestSimulated => {
                Self::TestSimulated(SimulatedBackend::new(seed))
            }
            BackendSelection::Noop => Self::Noop,
        }
    }

    fn build_kernel_preferred(seed: u64) -> Self {
        #[cfg(all(target_os = "linux", feature = "kernel-probes"))]
        {
            return Self::KernelLive(KernelLiveSlot::new_deferred());
        }
        #[cfg(not(all(target_os = "linux", feature = "kernel-probes")))]
        {
            let _ = seed;
            Self::KernelSimulated(KernelSimulatedBackend::new(seed))
        }
    }

    pub fn attach(&mut self) -> Result<(), String> {
        match self {
            Self::KernelLive(slot) => slot.attach(),
            Self::KernelSimulated(b) => b.attach(),
            Self::Tap(b) => b.attach(),
            Self::TestSimulated(b) => b.attach(),
            Self::Noop => Ok(()),
        }
    }

    pub fn detach(&mut self) -> bool {
        match self {
            Self::KernelLive(slot) => slot.detach(),
            Self::KernelSimulated(b) => b.detach(),
            Self::Tap(b) => b.detach(),
            Self::TestSimulated(b) => b.detach(),
            Self::Noop => false,
        }
    }

    pub fn is_attached(&self) -> bool {
        match self {
            Self::KernelLive(slot) => slot.is_attached(),
            Self::KernelSimulated(b) => b.is_attached(),
            Self::Tap(b) => b.is_attached(),
            Self::TestSimulated(b) => b.is_attached(),
            Self::Noop => false,
        }
    }

    pub fn backend_name(&self) -> &'static str {
        match self {
            Self::KernelLive(slot) => slot.backend_name(),
            Self::KernelSimulated(b) => b.backend_name(),
            Self::Tap(b) => b.backend_name(),
            Self::TestSimulated(b) => b.backend_name(),
            Self::Noop => "noop",
        }
    }

    pub fn kernel_streaming(&self) -> bool {
        match self {
            Self::KernelLive(slot) => slot.kernel_streaming(),
            Self::KernelSimulated(b) => b.kernel_streaming(),
            Self::Tap(_) | Self::TestSimulated(_) | Self::Noop => false,
        }
    }

    pub fn is_kernel_family(&self) -> bool {
        matches!(self, Self::KernelLive(_) | Self::KernelSimulated(_))
    }

    pub fn drain_events(&mut self) -> Vec<ProbeEvent> {
        match self {
            Self::KernelLive(slot) => slot.drain_events(),
            Self::KernelSimulated(b) => b.drain_events(),
            Self::Tap(b) => b.drain_events(),
            Self::TestSimulated(b) => b.drain_events(),
            Self::Noop => Vec::new(),
        }
    }

    pub fn sync_wal_activity(&mut self, delta_total_us: u64, max_us: u64) {
        match self {
            Self::KernelSimulated(b) => b.sync_wal_activity(delta_total_us, max_us),
            Self::KernelLive(_) => {}
            Self::Tap(b) => {
                if delta_total_us > 0 {
                    let ts = 1;
                    b.report_fsync(SyscallKind::Fsync, max_us.max(1), ts);
                    if delta_total_us > max_us {
                        b.report_fsync(
                            SyscallKind::Fdatasync,
                            (delta_total_us - max_us).max(1),
                            ts + 1,
                        );
                    }
                }
            }
            Self::TestSimulated(_) | Self::Noop => {}
        }
    }

    pub fn report_fsync(&mut self, syscall: SyscallKind, latency_us: u64, ts_ns: u64) {
        match self {
            Self::Tap(b) => b.report_fsync(syscall, latency_us, ts_ns),
            Self::KernelSimulated(b) => {
                if b.is_attached() {
                    b.sync_wal_activity(latency_us, latency_us);
                }
            }
            Self::KernelLive(_) | Self::TestSimulated(_) | Self::Noop => {}
        }
    }
}

#[cfg(all(target_os = "linux", feature = "kernel-probes"))]
impl KernelLiveSlot {
    fn attach(&mut self) -> Result<(), String> {
        if self.inner.is_some() {
            return Ok(());
        }
        match KernelBackend::try_attach() {
            Ok(k) => {
                self.inner = Some(k);
                Ok(())
            }
            Err(e) => {
                self.attach_error = Some(e.clone());
                Err(e)
            }
        }
    }

    fn detach(&mut self) -> bool {
        self.inner.as_mut().is_some_and(|k| k.detach())
    }

    fn is_attached(&self) -> bool {
        self.inner.as_ref().is_some_and(|k| k.is_attached())
    }

    fn backend_name(&self) -> &'static str {
        if self.is_attached() {
            "kernel-live"
        } else {
            "kernel-live-unavailable"
        }
    }

    fn kernel_streaming(&self) -> bool {
        self.is_attached()
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        self.inner
            .as_mut()
            .map(|k| k.drain_events())
            .unwrap_or_default()
    }
}

#[cfg(not(all(target_os = "linux", feature = "kernel-probes")))]
impl KernelLiveSlot {
    fn attach(&mut self) -> Result<(), String> {
        Err("kernel live requires linux + kernel-probes".into())
    }

    fn detach(&mut self) -> bool {
        false
    }

    fn is_attached(&self) -> bool {
        false
    }

    fn backend_name(&self) -> &'static str {
        "kernel-live-unavailable"
    }

    fn kernel_streaming(&self) -> bool {
        false
    }

    fn drain_events(&mut self) -> Vec<ProbeEvent> {
        Vec::new()
    }
}

/// How `ProbeConfig` selects the backend slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendSelection {
    /// Server `--ebpf`: KernelLive when attach succeeds, else KernelSimulated.
    KernelPreferred,
    KernelSimulated,
    Tap,
    TestSimulated,
    Noop,
}

