// Benchmark: Fast, deterministic CI smoke benchmark.
//
// Run with: cargo bench --bench smoke -p kaya-bench

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;

use kaya_bench::run_smoke_put_get;

fn smoke_benchmarks(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("smoke");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(10));
    group.measurement_time(Duration::from_millis(50));
    group.throughput(Throughput::Elements(10));

    group.bench_function("smoke_put_get", |b| {
        b.iter(|| rt.block_on(run_smoke_put_get()))
    });

    group.finish();
}

criterion_group!(benches, smoke_benchmarks);
criterion_main!(benches);
