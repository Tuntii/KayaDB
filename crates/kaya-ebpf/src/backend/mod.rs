pub mod kernel;
pub mod kernel_sim;
pub mod probe_backend;
mod simulated;
mod tap;

#[cfg(target_os = "linux")]
pub use kernel::KernelBackend;
pub use kernel_sim::KernelSimulatedBackend;
pub use probe_backend::{BackendSelection, ProbeBackend};
pub use simulated::SimulatedBackend;
pub use tap::TapBackend;

use crate::event::{ProbeEvent, SyscallKind};

/// Legacy trait for tap/simulated helpers.
pub trait EventBackend: Send {
    fn attach(&mut self) -> Result<(), String>;
    fn detach(&mut self) -> bool;
    fn is_attached(&self) -> bool;
    fn backend_name(&self) -> &'static str;
    fn drain_events(&mut self) -> Vec<ProbeEvent>;
    fn report_fsync(&mut self, _syscall: SyscallKind, _latency_us: u64, _ts_ns: u64) {}
}