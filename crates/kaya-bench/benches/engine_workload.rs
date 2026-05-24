// Benchmark: end-to-end engine PUT/GET/DELETE on SimDisk.
//
// Run with: cargo bench --bench engine_workload -p kaya-bench
//
// Labels durability mode truthfully per BENCH-001.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, WriteOptions};
use kaya_io::SimDisk;
use std::{path::PathBuf, sync::Arc};

const OPS: u64 = 500;

fn sim_engine_config() -> EngineConfig {
    EngineConfig {
        data_dir: PathBuf::new(),
        disable_locking: true,
        ..EngineConfig::default()
    }
}

fn engine_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("engine_workload");
    group.throughput(Throughput::Elements(OPS));

    let relaxed_opts = WriteOptions {
        durability: Some(DurabilityMode::Relaxed),
        idempotency_key: None,
    };
    let strict_opts = WriteOptions {
        durability: Some(DurabilityMode::Strict),
        idempotency_key: None,
    };

    // ── relaxed PUT × 500 ────────────────────────────────────────────────────
    group.bench_function("put_relaxed_500ops", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                for i in 0u16..500 {
                    let key = format!("k{i:04}").into_bytes();
                    engine
                        .put(key, vec![0xab; 32], relaxed_opts.clone())
                        .await
                        .unwrap();
                }
            })
        })
    });

    // ── strict PUT × 500 ─────────────────────────────────────────────────────
    group.bench_function("put_strict_500ops", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                for i in 0u16..500 {
                    let key = format!("k{i:04}").into_bytes();
                    engine
                        .put(key, vec![0xab; 32], strict_opts.clone())
                        .await
                        .unwrap();
                }
            })
        })
    });

    // ── hot GET × 500 (memtable only) ────────────────────────────────────────
    group.bench_function("get_hot_500ops", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                // Seed 500 keys.
                for i in 0u16..500 {
                    let key = format!("k{i:04}").into_bytes();
                    engine
                        .put(key, vec![0xab; 32], relaxed_opts.clone())
                        .await
                        .unwrap();
                }
                // Read them back (memtable hot path).
                for i in 0u16..500 {
                    let key = format!("k{i:04}").into_bytes();
                    engine.get(&key, ReadOptions::default()).await.unwrap();
                }
            })
        })
    });

    group.finish();
}

criterion_group!(benches, engine_benchmarks);
criterion_main!(benches);
