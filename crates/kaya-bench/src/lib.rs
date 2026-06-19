//! Benchmark harness and reporting utilities for KayaDB.

pub mod report;

pub use report::BenchmarkReport;

use std::path::PathBuf;
use std::sync::Arc;

use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, WriteOptions};
use kaya_io::SimDisk;

/// Shared smoke workload (10 put + 10 get) used by both the criterion smoke bench
/// and the CI performance regression gate. Uses SimDisk + relaxed durability.
pub async fn run_smoke_put_get() {
    let disk = Arc::new(SimDisk::new());
    let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();

    let relaxed_opts = WriteOptions {
        durability: Some(DurabilityMode::Relaxed),
        idempotency_key: None,
    };

    for i in 0u16..10 {
        let key = format!("k{i:04}").into_bytes();
        engine
            .put(key.clone(), vec![0xab; 16], relaxed_opts.clone())
            .await
            .unwrap();
        engine.get(&key, ReadOptions::default()).await.unwrap();
    }
}

fn sim_engine_config() -> EngineConfig {
    EngineConfig {
        data_dir: PathBuf::new(),
        disable_locking: true,
        ..EngineConfig::default()
    }
}
