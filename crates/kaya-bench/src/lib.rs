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

/// Multi-key SI transaction smoke (M25 perf envelope v2).
///
/// Begins one transaction, stages 8 puts, commits via `txn_commit`, then verifies
/// each key. Coarse regression gate for the local SI multi-key path.
pub async fn run_smoke_txn_multi_key() {
    let disk = Arc::new(SimDisk::new());
    let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();

    let (txn_id, _) = engine.begin_txn();
    for i in 0u8..8 {
        let key = format!("mk{i:02}").into_bytes();
        engine.txn_put(txn_id, key, vec![0xcd; 16]).unwrap();
    }
    engine.txn_commit(txn_id).await.unwrap();

    for i in 0u8..8 {
        let key = format!("mk{i:02}").into_bytes();
        let got = engine.get(&key, ReadOptions::default()).await.unwrap();
        assert_eq!(got.as_deref(), Some(&[0xcd; 16][..]));
    }
}

/// Multi-range participant 2PC smoke (M25 perf envelope v2).
///
/// Simulates keys that would live on opposite sides of a range split (`a*` vs
/// `m*`) by running prepare + commit_2pc on the shared engine. Full cross-group
/// coordinator / Raft path stays in server integration tests; this gate catches
/// gross regressions on the 2PC record + materialization hot path.
pub async fn run_smoke_multi_range_2pc() {
    let disk = Arc::new(SimDisk::new());
    let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();

    // Two "ranges": keys < "m" and keys >= "m" (mirrors StaticRangeTable split_at b"m").
    let mutations = vec![
        (b"a-acct-1".to_vec(), Some(b"100".to_vec())),
        (b"a-acct-2".to_vec(), Some(b"200".to_vec())),
        (b"m-acct-1".to_vec(), Some(b"300".to_vec())),
        (b"m-acct-2".to_vec(), Some(b"400".to_vec())),
    ];

    let txn_id = 42u64;
    engine.apply_txn_prepare(txn_id, &mutations).await.unwrap();
    engine.apply_txn_commit_2pc(txn_id).await.unwrap();

    for (key, want) in &mutations {
        let got = engine.get(key, ReadOptions::default()).await.unwrap();
        assert_eq!(got.as_ref(), want.as_ref());
    }
}

fn sim_engine_config() -> EngineConfig {
    EngineConfig {
        data_dir: PathBuf::new(),
        disable_locking: true,
        ..EngineConfig::default()
    }
}
