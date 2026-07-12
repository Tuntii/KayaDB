// Benchmark: large-value, high-key-count, and mixed read/write/scan workloads.
//
// Run with: cargo bench --bench mixed_workload -p kaya-bench
//
// Complements engine_workload.rs (small-value hot path) with the workload
// shapes flagged in ROADMAP Track F: large values, higher key counts (flush +
// cold SSTable reads), and a mixed put/get/delete/scan pattern.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, ScanOptions, WriteOptions};
use kaya_io::SimDisk;
use std::{path::PathBuf, sync::Arc};

fn sim_engine_config() -> EngineConfig {
    EngineConfig {
        data_dir: PathBuf::new(),
        disable_locking: true,
        ..EngineConfig::default()
    }
}

fn relaxed() -> WriteOptions {
    WriteOptions {
        durability: Some(DurabilityMode::Relaxed),
        idempotency_key: None,
    }
}

fn mixed_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // ── large values: 64 KiB payloads × 200 ops ──────────────────────────────
    {
        const OPS: u64 = 200;
        const VALUE_LEN: usize = 64 * 1024;
        let mut group = c.benchmark_group("mixed_large_value");
        group.throughput(Throughput::Bytes(OPS * VALUE_LEN as u64));
        group.sample_size(20);
        group.bench_function("put_relaxed_64kib_200ops", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let disk = Arc::new(SimDisk::new());
                    let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                    for i in 0u16..OPS as u16 {
                        let key = format!("big{i:04}").into_bytes();
                        engine
                            .put(key, vec![0xcd; VALUE_LEN], relaxed())
                            .await
                            .unwrap();
                    }
                })
            })
        });
        group.finish();
    }

    // ── high key count: 5 000 keys (forces flush + cold SSTable reads) ────────
    {
        const OPS: u64 = 5_000;
        let mut group = c.benchmark_group("mixed_high_key_count");
        group.throughput(Throughput::Elements(OPS));
        group.sample_size(10);
        group.bench_function("put_get_5000_keys", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let disk = Arc::new(SimDisk::new());
                    let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                    for i in 0u32..OPS as u32 {
                        let key = format!("k{i:06}").into_bytes();
                        engine.put(key, vec![0xab; 64], relaxed()).await.unwrap();
                    }
                    engine.flush().await.unwrap();
                    // Cold reads through flushed SSTables.
                    for i in 0u32..OPS as u32 {
                        let key = format!("k{i:06}").into_bytes();
                        engine.get(&key, ReadOptions::default()).await.unwrap();
                    }
                })
            })
        });
        group.finish();
    }

    // ── mixed workload: interleaved put/get/delete + scan over 1 000 keys ─────
    {
        const OPS: u64 = 1_000;
        let mut group = c.benchmark_group("mixed_rw_scan");
        group.throughput(Throughput::Elements(OPS));
        group.sample_size(20);
        group.bench_function("interleaved_put_get_delete_scan", |b| {
            b.iter(|| {
                rt.block_on(async {
                    let disk = Arc::new(SimDisk::new());
                    let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                    for i in 0u16..OPS as u16 {
                        let key = format!("m{i:04}").into_bytes();
                        engine
                            .put(key.clone(), vec![0x11; 48], relaxed())
                            .await
                            .unwrap();
                        if i % 4 == 0 {
                            engine.get(&key, ReadOptions::default()).await.unwrap();
                        }
                        if i % 8 == 0 {
                            engine.delete(key, relaxed()).await.unwrap();
                        }
                    }
                    // A prefix scan across the surviving key space.
                    engine
                        .scan_prefix(b"m0", ScanOptions::default())
                        .await
                        .unwrap();
                })
            })
        });
        group.finish();
    }
}

criterion_group!(benches, mixed_benchmarks);
criterion_main!(benches);
