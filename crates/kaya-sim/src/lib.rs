use kaya_core::{KayaError, Result};
pub use kaya_io::{FaultRule, FaultSchedule, SimDisk, SimDiskEvent, SimSeed};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationConfig {
    pub seed: SimSeed,
    pub max_operations: u64,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            seed: SimSeed(0xdead_beef),
            max_operations: 1_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimulationReport {
    pub seed: SimSeed,
    pub operations_executed: u64,
    pub invariant_failures: Vec<String>,
}

pub fn run_small_seed_suite(config: SimulationConfig) -> Result<SimulationReport> {
    if config.max_operations == 0 {
        return Err(KayaError::invalid_argument(
            "simulation max_operations must be greater than zero",
        ));
    }
    Ok(SimulationReport {
        seed: config.seed,
        operations_executed: 0,
        invariant_failures: Vec::new(),
    })
}
