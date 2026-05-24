// Benchmark: Fast, deterministic CI smoke benchmark.
//
// Run with: cargo bench --bench smoke -p kaya-bench

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, WriteOptions};
use kaya_io::SimDisk;
use std::{path::PathBuf, sync::Arc, time::Duration};

fn sim_engine_config() -> EngineConfig {
    EngineConfig {
        data_dir: PathBuf::new(),
        disable_locking: true,
        ..EngineConfig::default()
    }
}

fn smoke_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("smoke");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(10));
    group.measurement_time(Duration::from_millis(50));
    group.throughput(Throughput::Elements(10));

    let relaxed_opts = WriteOptions {
        durability: Some(DurabilityMode::Relaxed),
        idempotency_key: None,
    };

    group.bench_function("smoke_put_get", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let mut engine = Engine::open(sim_engine_config(), disk).await.unwrap();
                for i in 0u16..10 {
                    let key = format!("k{i:04}").into_bytes();
                    engine
                        .put(key.clone(), vec![0xab; 16], relaxed_opts.clone())
                        .await
                        .unwrap();
                    engine.get(&key, ReadOptions::default()).await.unwrap();
                }
            })
        })
    });

    group.finish();
}

criterion_group!(benches, smoke_benchmarks);
criterion_main!(benches);
