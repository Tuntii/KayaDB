// Benchmark: SSTable build and scan throughput.
//
// Run with: cargo bench --bench sstable -p kaya-bench

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use kaya_core::SequenceNumber;
use kaya_lsm::{SstEntry, SstableBuilder, SstableReader};

const N: usize = 1_000;

fn build_table(n: usize) -> Vec<u8> {
    let mut builder = SstableBuilder::new(4096);
    for i in 0..n {
        builder.add(SstEntry {
            key: format!("key:{i:06}").into_bytes(),
            value: Some(vec![0xab; 32]),
            sequence: SequenceNumber::new(i as u64 + 1),
        });
    }
    builder.finish().unwrap()
}

fn sstable_benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable");
    group.throughput(Throughput::Elements(N as u64));

    // ── build 1 000 entries ───────────────────────────────────────────────────
    group.bench_function("build_1000_32b_values", |b| b.iter(|| build_table(N)));

    // ── full scan 1 000 entries ───────────────────────────────────────────────
    let bytes = build_table(N);
    group.bench_function("scan_prefix_1000_entries", |b| {
        b.iter(|| {
            let reader = SstableReader::open(bytes.clone()).unwrap();
            reader.scan_prefix(b"key:").unwrap()
        })
    });

    // ── point get (hot path — last key) ──────────────────────────────────────
    let last_key = format!("key:{:06}", N - 1).into_bytes();
    group.bench_function("get_last_key", |b| {
        b.iter(|| {
            let reader = SstableReader::open(bytes.clone()).unwrap();
            reader.get(&last_key).unwrap()
        })
    });

    group.finish();
}

criterion_group!(benches, sstable_benchmarks);
criterion_main!(benches);
