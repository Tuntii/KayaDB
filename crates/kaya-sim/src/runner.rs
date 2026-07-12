use std::sync::Arc;

use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, ReadTimestamp, ScanOptions, WriteOptions};
use kaya_io::SimDisk;

use crate::{
    model::RefModel,
    rng::SimRng,
    trace::{hex_enc, parse_trace, ParsedOp, ParsedResult, TraceLine, TraceWriter},
    SimulationConfig, SimulationReport,
};

// ── Simulation runner ─────────────────────────────────────────────────────────

pub(crate) async fn run_async(config: SimulationConfig) -> SimulationReport {
    let disk = Arc::new(SimDisk::new());
    let engine_cfg = EngineConfig {
        disable_locking: true,
        ..Default::default()
    };
    let mut engine = Engine::open(engine_cfg.clone(), disk.clone())
        .await
        .expect("engine open");

    let mut rng = SimRng::new(config.seed.0);
    let mut model = RefModel::new();
    let mut tw = TraceWriter::new();
    let mut violations: Vec<String> = Vec::new();
    let mut op_id: u64 = 0;

    let strict = WriteOptions {
        durability: Some(DurabilityMode::Strict),
        ..WriteOptions::default()
    };

    // Operation weights: [put, get, delete, scan, flush, compact, crash_restart]
    let weights = [
        config.put_weight,
        config.get_weight,
        config.delete_weight,
        config.scan_weight,
        config.flush_weight,
        config.compact_weight,
        config.crash_weight,
    ];

    tw.sim_start(config.seed.0, config.max_operations);

    for _ in 0..config.max_operations {
        op_id += 1;
        let choice = rng.weighted_index(&weights);

        match choice {
            // ── PUT ───────────────────────────────────────────────────────────
            0 => {
                let key = gen_key(&mut rng, config.keyspace_size);
                let vlen = 1 + rng.usize_below(config.max_value_bytes);
                let value = rng.bytes(vlen);
                tw.op_put(op_id, &key, &value);
                match engine.put(key.clone(), value.clone(), strict.clone()).await {
                    Ok(wr) => {
                        model.put(key.clone(), value.clone(), wr.sequence.get());
                        tw.result_ok(op_id);
                        let actual = engine
                            .get(&key, ReadOptions::default())
                            .await
                            .unwrap_or(None);
                        check_eng002(
                            &key,
                            model.get(&key).cloned(),
                            actual,
                            &mut violations,
                            &mut tw,
                        );
                    }
                    Err(e) => {
                        tw.result_ok(op_id);
                        violations.push(format!("PUT unexpected error: {e}"));
                    }
                }
            }

            // ── GET ───────────────────────────────────────────────────────────
            1 => {
                let key = gen_key(&mut rng, config.keyspace_size);
                tw.op_get(op_id, &key);
                let actual = engine
                    .get(&key, ReadOptions::default())
                    .await
                    .unwrap_or(None);
                tw.result_get(op_id, actual.as_deref());
                check_eng002(
                    &key,
                    model.get(&key).cloned(),
                    actual,
                    &mut violations,
                    &mut tw,
                );
            }

            // ── DELETE ────────────────────────────────────────────────────────
            2 => {
                let key = gen_key(&mut rng, config.keyspace_size);
                tw.op_delete(op_id, &key);
                match engine.delete(key.clone(), strict.clone()).await {
                    Ok(wr) => {
                        model.delete(&key, wr.sequence.get());
                        tw.result_ok(op_id);
                        let actual = engine
                            .get(&key, ReadOptions::default())
                            .await
                            .unwrap_or(None);
                        if actual.is_some() {
                            let d = format!("ENG-003: key {} visible after delete", hex_enc(&key));
                            violations.push(d.clone());
                            tw.invariant_violation("ENG-003", &d);
                        } else {
                            tw.invariant_ok("ENG-003");
                        }
                    }
                    Err(e) => {
                        tw.result_ok(op_id);
                        violations.push(format!("DELETE unexpected error: {e}"));
                    }
                }
            }

            // ── SCAN ──────────────────────────────────────────────────────────
            3 => {
                let prefix = b"key:".to_vec();
                tw.op_scan(op_id, &prefix);
                let items = engine
                    .scan_prefix(&prefix, ScanOptions::default())
                    .await
                    .unwrap_or_default();
                tw.result_scan(op_id, items.len());
                let expected = model.scan_prefix(&prefix);
                let actual_pairs: Vec<(Vec<u8>, Vec<u8>)> =
                    items.into_iter().map(|kv| (kv.key, kv.value)).collect();
                if expected != actual_pairs {
                    let d = format!(
                        "ENG-004: scan prefix='{}': expected {} entries got {}",
                        hex_enc(&prefix),
                        expected.len(),
                        actual_pairs.len()
                    );
                    violations.push(d.clone());
                    tw.invariant_violation("ENG-004", &d);
                } else {
                    tw.invariant_ok("ENG-004");
                }
            }

            // ── FLUSH ─────────────────────────────────────────────────────────
            4 => {
                tw.op_flush(op_id);
                let _ = engine.flush().await;
                tw.result_ok(op_id);
            }

            // ── COMPACT ───────────────────────────────────────────────────────
            5 => {
                tw.op_compact(op_id);
                let _ = engine.compact().await;
                tw.result_ok(op_id);
            }

            // ── CRASH + RESTART ───────────────────────────────────────────────
            _ => {
                tw.op_crash_restart(op_id);
                engine.close().await.ok();
                tw.crash_event();
                disk.crash();
                engine = Engine::open(engine_cfg.clone(), disk.clone())
                    .await
                    .expect("engine reopen after crash");
                tw.restart_event();
                tw.result_ok(op_id);

                // ENG-001: every key in the keyspace must match the reference model (Latest).
                let mut all_ok = true;
                for idx in 0..config.keyspace_size {
                    let key = format!("key:{idx:04x}").into_bytes();
                    let expected = model.get(&key).cloned();
                    let actual = engine
                        .get(&key, ReadOptions::default())
                        .await
                        .unwrap_or(None);
                    if expected != actual {
                        let d = format!(
                            "ENG-001: key {}: expected {:?}, got {:?}",
                            hex_enc(&key),
                            expected,
                            actual
                        );
                        violations.push(d.clone());
                        tw.invariant_violation("ENG-001", &d);
                        all_ok = false;
                    }
                }
                if all_ok {
                    tw.invariant_ok("ENG-001");
                }

                // MVCC property: for each recorded commit_ts, engine get_at
                // matches the versioned RefModel after crash/restart.
                check_mvcc_get_at(
                    &mut engine,
                    &model,
                    config.keyspace_size,
                    &mut violations,
                    &mut tw,
                )
                .await;
            }
        }
    }

    engine.close().await.ok();

    SimulationReport {
        seed: config.seed,
        operations_executed: op_id,
        invariant_failures: violations,
        trace: tw.finish(),
    }
}

/// After crash/restart (or any durable point), verify snapshot reads at every
/// known commit timestamp match the multi-version reference model.
async fn check_mvcc_get_at(
    engine: &mut Engine<SimDisk>,
    model: &RefModel,
    keyspace_size: u64,
    violations: &mut Vec<String>,
    tw: &mut TraceWriter,
) {
    let timestamps = model.all_commit_timestamps();
    if timestamps.is_empty() {
        tw.invariant_ok("MVCC-001");
        return;
    }

    let mut all_ok = true;
    for idx in 0..keyspace_size {
        let key = format!("key:{idx:04x}").into_bytes();
        // Only probe timestamps that matter for this key, plus a couple global ones.
        let mut read_ts_list = model.versions_of(&key);
        // Also sample a few global timestamps so cross-key ordering is covered.
        for &ts in timestamps.iter().take(4) {
            if !read_ts_list.contains(&ts) {
                read_ts_list.push(ts);
            }
        }
        read_ts_list.sort_unstable();
        read_ts_list.dedup();

        for read_ts in read_ts_list {
            let expected = model.get_at(&key, read_ts).cloned();
            let actual = engine
                .get(
                    &key,
                    ReadOptions {
                        read_at: ReadTimestamp::At(read_ts),
                    },
                )
                .await
                .unwrap_or(None);
            if expected != actual {
                let d = format!(
                    "MVCC-001: key {} @ read_ts={read_ts}: expected {:?}, got {:?}",
                    hex_enc(&key),
                    expected,
                    actual
                );
                violations.push(d.clone());
                tw.invariant_violation("MVCC-001", &d);
                all_ok = false;
            }
        }
    }
    if all_ok {
        tw.invariant_ok("MVCC-001");
    }
}

// ── Replay ────────────────────────────────────────────────────────────────────

pub(crate) async fn replay_async(trace_jsonl: &str) -> Result<(), String> {
    let lines = parse_trace(trace_jsonl);

    let disk = Arc::new(SimDisk::new());
    let engine_cfg = EngineConfig {
        disable_locking: true,
        ..Default::default()
    };
    let mut engine = Engine::open(engine_cfg.clone(), disk.clone())
        .await
        .map_err(|e| e.to_string())?;

    let strict = WriteOptions {
        durability: Some(DurabilityMode::Strict),
        ..WriteOptions::default()
    };

    // Pending results from the most recently executed op, for comparison
    // against the matching op_result trace line.
    let mut pending_get: Option<(u64, Option<Vec<u8>>)> = None;
    let mut pending_scan: Option<(u64, usize)> = None;
    let mut divergences: Vec<String> = Vec::new();

    for line in &lines {
        match line {
            TraceLine::Op(op) => {
                pending_get = None;
                pending_scan = None;
                match op {
                    ParsedOp::Put { key, val, .. } => {
                        engine
                            .put(key.clone(), val.clone(), strict.clone())
                            .await
                            .ok();
                    }
                    ParsedOp::Get { oid, key } => {
                        let result = engine
                            .get(key, ReadOptions::default())
                            .await
                            .unwrap_or(None);
                        pending_get = Some((*oid, result));
                    }
                    ParsedOp::Delete { key, .. } => {
                        engine.delete(key.clone(), strict.clone()).await.ok();
                    }
                    ParsedOp::Scan { oid, prefix } => {
                        let items = engine
                            .scan_prefix(prefix, ScanOptions::default())
                            .await
                            .unwrap_or_default();
                        pending_scan = Some((*oid, items.len()));
                    }
                    ParsedOp::Flush { .. } => {
                        engine.flush().await.ok();
                    }
                    ParsedOp::Compact { .. } => {
                        engine.compact().await.ok();
                    }
                    ParsedOp::CrashRestart { .. } => {
                        engine.close().await.ok();
                        disk.crash();
                        engine = Engine::open(engine_cfg.clone(), disk.clone())
                            .await
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
            TraceLine::OpResult(result) => match result {
                ParsedResult::Get {
                    oid,
                    value: expected,
                } => {
                    if let Some((pid, actual)) = &pending_get {
                        if pid == oid && actual != expected {
                            divergences.push(format!(
                                "oid {oid}: GET diverged: expected {:?}, got {:?}",
                                expected, actual
                            ));
                        }
                    }
                }
                ParsedResult::Scan {
                    oid,
                    count: expected_count,
                } => {
                    if let Some((pid, actual_count)) = &pending_scan {
                        if pid == oid && actual_count != expected_count {
                            divergences.push(format!(
                                "oid {oid}: SCAN count diverged: expected {expected_count}, got {actual_count}"
                            ));
                        }
                    }
                }
                ParsedResult::Void { .. } => {}
            },
            _ => {}
        }
    }

    engine.close().await.ok();

    if divergences.is_empty() {
        Ok(())
    } else {
        Err(divergences.join("\n"))
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn gen_key(rng: &mut SimRng, keyspace_size: u64) -> Vec<u8> {
    let idx = rng.next_u64() % keyspace_size;
    format!("key:{idx:04x}").into_bytes()
}

fn check_eng002(
    key: &[u8],
    expected: Option<Vec<u8>>,
    actual: Option<Vec<u8>>,
    violations: &mut Vec<String>,
    tw: &mut TraceWriter,
) {
    if expected == actual {
        tw.invariant_ok("ENG-002");
    } else {
        let d = format!(
            "ENG-002: key {}: expected {:?}, got {:?}",
            hex_enc(key),
            expected,
            actual
        );
        violations.push(d.clone());
        tw.invariant_violation("ENG-002", &d);
    }
}

// ── Integration-style MVCC crash property ─────────────────────────────────────

#[cfg(test)]
mod mvcc_tests {
    use super::*;

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(f)
    }

    fn read_at(ts: u64) -> ReadOptions {
        ReadOptions {
            read_at: ReadTimestamp::At(ts),
        }
    }

    #[test]
    fn mvcc_multi_version_put_crash_get_at_matches_model() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let engine_cfg = EngineConfig {
                disable_locking: true,
                ..Default::default()
            };
            let mut engine = Engine::open(engine_cfg.clone(), disk.clone())
                .await
                .expect("open");
            let mut model = RefModel::new();

            let strict = WriteOptions {
                durability: Some(DurabilityMode::Strict),
                ..WriteOptions::default()
            };

            let key = b"key:0001".to_vec();
            let w1 = engine
                .put(key.clone(), b"v1".to_vec(), strict.clone())
                .await
                .unwrap();
            model.put(key.clone(), b"v1".to_vec(), w1.sequence.get());

            let w2 = engine
                .put(key.clone(), b"v2".to_vec(), strict.clone())
                .await
                .unwrap();
            model.put(key.clone(), b"v2".to_vec(), w2.sequence.get());

            let w3 = engine
                .put(key.clone(), b"v3".to_vec(), strict.clone())
                .await
                .unwrap();
            model.put(key.clone(), b"v3".to_vec(), w3.sequence.get());

            // Crash mid-history and reopen.
            engine.close().await.ok();
            disk.crash();
            let mut engine = Engine::open(engine_cfg, disk).await.expect("reopen");

            for ts in [w1.sequence.get(), w2.sequence.get(), w3.sequence.get()] {
                let expected = model.get_at(&key, ts).cloned();
                let actual = engine.get(&key, read_at(ts)).await.unwrap();
                assert_eq!(
                    expected, actual,
                    "get_at mismatch after crash at read_ts={ts}"
                );
            }

            // Latest still matches.
            assert_eq!(
                engine.get(&key, ReadOptions::default()).await.unwrap(),
                model.get(&key).cloned()
            );
            assert_eq!(
                engine.get(&key, ReadOptions::default()).await.unwrap(),
                Some(b"v3".to_vec())
            );
        });
    }

    #[test]
    fn mvcc_delete_then_crash_snapshot_sees_old_put() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let engine_cfg = EngineConfig {
                disable_locking: true,
                ..Default::default()
            };
            let mut engine = Engine::open(engine_cfg.clone(), disk.clone())
                .await
                .expect("open");
            let mut model = RefModel::new();

            let strict = WriteOptions {
                durability: Some(DurabilityMode::Strict),
                ..WriteOptions::default()
            };

            let key = b"key:00aa".to_vec();
            let w1 = engine
                .put(key.clone(), b"alive".to_vec(), strict.clone())
                .await
                .unwrap();
            model.put(key.clone(), b"alive".to_vec(), w1.sequence.get());

            let w2 = engine.delete(key.clone(), strict).await.unwrap();
            model.delete(&key, w2.sequence.get());

            engine.close().await.ok();
            disk.crash();
            let mut engine = Engine::open(engine_cfg, disk).await.expect("reopen");

            assert_eq!(
                engine.get(&key, read_at(w1.sequence.get())).await.unwrap(),
                Some(b"alive".to_vec())
            );
            assert_eq!(
                model.get_at(&key, w1.sequence.get()).map(|v| v.as_slice()),
                Some(b"alive".as_ref())
            );
            assert_eq!(
                engine.get(&key, read_at(w2.sequence.get())).await.unwrap(),
                None
            );
            assert_eq!(
                engine.get(&key, ReadOptions::default()).await.unwrap(),
                None
            );
            assert_eq!(model.get(&key), None);
        });
    }
}
