// Benchmark: WAL append throughput (relaxed vs. strict) on SimDisk.
//
// Run with: cargo bench --bench wal_append -p kaya-bench
//
// Hardware context should be recorded alongside results per BENCH-002.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kaya_core::{DurabilityMode, WalConfig};
use kaya_io::SimDisk;
use kaya_wal::{WalPayload, WalWriter};
use std::sync::Arc;

const OPS_PER_ITER: u64 = 200;

fn wal_append_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_append");
    group.throughput(Throughput::Elements(OPS_PER_ITER));

    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    // ── relaxed: no fsync per record ──────────────────────────────────────────
    group.bench_function("relaxed_200ops_64b", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let w = WalWriter::open(WalConfig::default(), disk).await.unwrap();
                for i in 0u8..200 {
                    w.append(
                        WalPayload::Put {
                            key: vec![i],
                            value: vec![0xab; 64],
                        },
                        DurabilityMode::Relaxed,
                    )
                    .await
                    .unwrap();
                }
            })
        })
    });

    // ── strict: fsync per record ──────────────────────────────────────────────
    group.bench_function("strict_200ops_64b", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let w = WalWriter::open(WalConfig::default(), disk).await.unwrap();
                for i in 0u8..200 {
                    w.append(
                        WalPayload::Put {
                            key: vec![i],
                            value: vec![0xab; 64],
                        },
                        DurabilityMode::Strict,
                    )
                    .await
                    .unwrap();
                }
            })
        })
    });

    // ── recovery: replay N strict records ────────────────────────────────────
    group.bench_function("recovery_200records", |b| {
        b.iter(|| {
            rt.block_on(async {
                let disk = Arc::new(SimDisk::new());
                let config = WalConfig::default();
                {
                    let w = WalWriter::open(config.clone(), disk.clone()).await.unwrap();
                    for i in 0u8..200 {
                        w.append(
                            WalPayload::Put {
                                key: vec![i],
                                value: vec![0xab; 64],
                            },
                            DurabilityMode::Strict,
                        )
                        .await
                        .unwrap();
                    }
                }
                kaya_wal::recover_wal(config, disk).await.unwrap()
            })
        })
    });

    group.finish();
}

criterion_group!(benches, wal_append_benchmarks);
criterion_main!(benches);
