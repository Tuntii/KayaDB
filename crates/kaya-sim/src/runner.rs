use std::sync::Arc;

use kaya_core::{DurabilityMode, EngineConfig};
use kaya_engine::{Engine, ReadOptions, ScanOptions, WriteOptions};
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
                    Ok(_) => {
                        model.put(key.clone(), value.clone());
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
                    Ok(_) => {
                        model.delete(&key);
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

                // ENG-001: every key in the keyspace must match the reference model.
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
