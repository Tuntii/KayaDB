use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use kaya_core::{DurabilityMode, EngineConfig, KayaError, Result, WalConfig};
use kaya_engine::{
    recover as engine_recover, Engine, EngineStats, ReadOptions, RecoveryReport, ScanOptions,
    WriteOptions,
};
use kaya_io::FileDisk;
use kaya_lsm::{inspect_manifest_path, inspect_sstable_path, ManifestInspection, SstInspection};
use kaya_net::{
    decode_error_payload, decode_scan_response, decode_value_payload, encode_key_payload,
    encode_put_payload, encode_scan_payload, roundtrip, STATUS_ERROR, STATUS_NOT_FOUND,
    STATUS_NOT_LEADER, STATUS_OK,
};
use kaya_wal::{inspect_wal_path, WalInspection};

fn main() {
    if let Err(error) = run() {
        eprintln!("ERROR: {error}");
        process::exit(error.exit_code());
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let json = remove_flag(&mut args, "--json");

    // --server <addr>: when present, all KV commands go over TCP.
    let server_addr: Option<SocketAddr> = match remove_value_flag(&mut args, "--server") {
        None => None,
        Some(s) => Some(
            s.parse()
                .map_err(|e| KayaError::invalid_argument(format!("--server: {e}")))?,
        ),
    };

    let data_dir = remove_value_flag(&mut args, "--data").unwrap_or_else(|| "./data".to_owned());
    let durability = match remove_value_flag(&mut args, "--durability").as_deref() {
        Some("relaxed") => DurabilityMode::Relaxed,
        Some("strict") | None => DurabilityMode::Strict,
        Some(other) => {
            return Err(KayaError::invalid_argument(format!(
                "unknown durability mode: {other}; expected strict or relaxed"
            )));
        }
    };

    // ── server mode ───────────────────────────────────────────────────────────
    if let Some(addr) = server_addr {
        return run_server_mode(args, addr, json);
    }

    // ── local engine mode (unchanged) ─────────────────────────────────────────
    match args.as_slice() {
        [] => {
            print_usage();
            Ok(())
        }
        [cmd] if cmd == "put" => Err(KayaError::invalid_argument(
            "usage: kayactl put <key> <value>",
        )),
        [cmd, key, value] if cmd == "put" => {
            let opts = WriteOptions {
                durability: Some(durability),
                idempotency_key: None,
            };
            let result = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine
                    .put(key.as_bytes().to_vec(), value.as_bytes().to_vec(), opts)
                    .await
            })?;
            if json {
                println!(
                    "{{\"ok\":true,\"sequence\":{},\"lsn\":{},\"durable\":{}}}",
                    result.sequence.get(),
                    result.lsn.get(),
                    result.durable
                );
            } else {
                println!(
                    "OK sequence={} lsn={} durable={}",
                    result.sequence.get(),
                    result.lsn.get(),
                    result.durable
                );
            }
            Ok(())
        }
        [cmd] if cmd == "get" => Err(KayaError::invalid_argument("usage: kayactl get <key>")),
        [cmd, key] if cmd == "get" => {
            let value = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine.get(key.as_bytes(), ReadOptions::default()).await
            })?;
            match value {
                Some(v) => {
                    let display = String::from_utf8_lossy(&v);
                    if json {
                        println!("{{\"found\":true,\"value\":{}}}", json_string(&display));
                    } else {
                        println!("{display}");
                    }
                    Ok(())
                }
                None => {
                    if json {
                        println!("{{\"found\":false}}");
                    } else {
                        println!("NOT_FOUND");
                    }
                    Err(KayaError::NotFound)
                }
            }
        }
        [cmd] if cmd == "delete" => Err(KayaError::invalid_argument("usage: kayactl delete <key>")),
        [cmd, key] if cmd == "delete" => {
            let opts = WriteOptions {
                durability: Some(durability),
                idempotency_key: None,
            };
            let result = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine.delete(key.as_bytes().to_vec(), opts).await
            })?;
            if json {
                println!(
                    "{{\"ok\":true,\"sequence\":{},\"lsn\":{},\"durable\":{}}}",
                    result.sequence.get(),
                    result.lsn.get(),
                    result.durable
                );
            } else {
                println!(
                    "OK sequence={} lsn={} durable={}",
                    result.sequence.get(),
                    result.lsn.get(),
                    result.durable
                );
            }
            Ok(())
        }
        [cmd] if cmd == "scan" => Err(KayaError::invalid_argument("usage: kayactl scan <prefix>")),
        [cmd, prefix] if cmd == "scan" => {
            let items = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine
                    .scan_prefix(prefix.as_bytes(), ScanOptions::default())
                    .await
            })?;
            if json {
                print!("{{\"items\":[");
                for (index, kv) in items.iter().enumerate() {
                    if index > 0 {
                        print!(",");
                    }
                    let k = String::from_utf8_lossy(&kv.key);
                    let v = String::from_utf8_lossy(&kv.value);
                    print!(
                        "{{\"key\":{},\"value\":{}}}",
                        json_string(&k),
                        json_string(&v)
                    );
                }
                println!("]}}");
            } else {
                for kv in &items {
                    let k = String::from_utf8_lossy(&kv.key);
                    let v = String::from_utf8_lossy(&kv.value);
                    println!("{k} {v}");
                }
            }
            Ok(())
        }
        [inspect, wal, path] if inspect == "inspect" && wal == "wal" => {
            let inspection = inspect_wal_path(path, WalConfig::default().max_record_bytes)?;
            if json {
                print_inspection_json(&inspection);
            } else {
                print_inspection_human(&inspection);
            }
            Ok(())
        }
        [inspect, sst, path] if inspect == "inspect" && sst == "sstable" => {
            let inspection = inspect_sstable_path(path)?;
            if json {
                print_sst_inspection_json(&inspection);
            } else {
                print_sst_inspection_human(&inspection);
            }
            Ok(())
        }
        [inspect, mani, path] if inspect == "inspect" && mani == "manifest" => {
            let inspection = inspect_manifest_path(path)?;
            if json {
                print_manifest_inspection_json(&inspection);
            } else {
                print_manifest_inspection_human(&inspection);
            }
            Ok(())
        }
        [cmd] if cmd == "stats" => {
            let engine = block_on(open_engine(data_dir, durability))?;
            let stats = engine.stats();
            let recovery = engine.last_recovery().clone();
            if json {
                print_stats_json(&stats, &recovery);
            } else {
                print_stats_human(&stats, &recovery);
            }
            Ok(())
        }
        [cmd, flag] if cmd == "recover" && flag == "--dry-run" => {
            use kaya_core::DurabilityConfig;
            let config = EngineConfig {
                data_dir: PathBuf::from(&data_dir),
                durability: DurabilityConfig {
                    mode: durability,
                    ..DurabilityConfig::default()
                },
                ..EngineConfig::default()
            };
            let disk = Arc::new(FileDisk::new(config.data_dir.clone()));
            let recovery = block_on(engine_recover(config, disk))?;
            if json {
                print_recovery_json(&recovery);
            } else {
                print_recovery_human(&recovery);
            }
            Ok(())
        }
        _ => Err(KayaError::invalid_argument("unknown kayactl command")),
    }
}

async fn open_engine(
    data_dir: String,
    default_durability: DurabilityMode,
) -> Result<Engine<FileDisk>> {
    use kaya_core::DurabilityConfig;
    let config = EngineConfig {
        data_dir: PathBuf::from(&data_dir),
        durability: DurabilityConfig {
            mode: default_durability,
            ..DurabilityConfig::default()
        },
        ..EngineConfig::default()
    };
    let disk = Arc::new(FileDisk::new(config.data_dir.clone()));
    Engine::open(config, disk).await
}

// ── server-mode dispatch ──────────────────────────────────────────────────────

async fn roundtrip_with_retry(
    mut addr: SocketAddr,
    opcode: u8,
    payload: &[u8],
) -> Result<(u16, Vec<u8>)> {
    let mut retries = 0;
    loop {
        let (status, body) = roundtrip(addr, opcode, payload)
            .await
            .map_err(|e| KayaError::internal(e.to_string()))?;
        if status == STATUS_NOT_LEADER && retries < 3 && !body.is_empty() {
            if let Ok(leader_addr_str) = String::from_utf8(body.clone()) {
                if let Ok(new_addr) = leader_addr_str.parse::<SocketAddr>() {
                    eprintln!("Redirecting to leader at {}...", new_addr);
                    addr = new_addr;
                    retries += 1;
                    continue;
                }
            }
        }
        return Ok((status, body));
    }
}

/// Send commands to a running `kayadb-server` over TCP.
fn run_server_mode(args: Vec<String>, addr: SocketAddr, json: bool) -> Result<()> {
    block_on(async move { run_server_mode_async(args, addr, json).await })
}

async fn run_server_mode_async(args: Vec<String>, addr: SocketAddr, json: bool) -> Result<()> {
    match args.as_slice() {
        [] => {
            print_usage();
            Ok(())
        }
        [cmd] if cmd == "put" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> put <key> <value>",
        )),
        [cmd, key, value] if cmd == "put" => {
            let payload = encode_put_payload(key.as_bytes(), value.as_bytes());
            let (status, body) = roundtrip_with_retry(addr, 1, &payload).await?;
            match status {
                STATUS_OK => {
                    if json {
                        println!("{{\"ok\":true}}");
                    } else {
                        println!("OK");
                    }
                    Ok(())
                }
                STATUS_NOT_LEADER => Err(KayaError::internal(
                    "not leader — retry on a different node",
                )),
                STATUS_ERROR => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::internal(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "get" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> get <key>",
        )),
        [cmd, key] if cmd == "get" => {
            let payload = encode_key_payload(key.as_bytes());
            let (status, body) = roundtrip_with_retry(addr, 2, &payload).await?;
            match status {
                STATUS_OK => {
                    let value = decode_value_payload(&body).map_err(KayaError::internal)?;
                    let display = String::from_utf8_lossy(&value);
                    if json {
                        println!("{{\"found\":true,\"value\":{}}}", json_string(&display));
                    } else {
                        println!("{display}");
                    }
                    Ok(())
                }
                STATUS_NOT_FOUND => {
                    if json {
                        println!("{{\"found\":false}}");
                    } else {
                        println!("NOT_FOUND");
                    }
                    Err(KayaError::NotFound)
                }
                STATUS_NOT_LEADER => Err(KayaError::internal(
                    "not leader — retry on a different node",
                )),
                STATUS_ERROR => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::internal(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "delete" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> delete <key>",
        )),
        [cmd, key] if cmd == "delete" => {
            let payload = encode_key_payload(key.as_bytes());
            let (status, body) = roundtrip_with_retry(addr, 3, &payload).await?;
            match status {
                STATUS_OK => {
                    if json {
                        println!("{{\"ok\":true}}");
                    } else {
                        println!("OK");
                    }
                    Ok(())
                }
                STATUS_NOT_LEADER => Err(KayaError::internal(
                    "not leader — retry on a different node",
                )),
                STATUS_ERROR => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::internal(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "scan" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> scan <prefix>",
        )),
        [cmd, prefix] if cmd == "scan" => {
            let payload = encode_scan_payload(prefix.as_bytes());
            let (status, body) = roundtrip_with_retry(addr, 4, &payload).await?;
            match status {
                STATUS_OK => {
                    let items = decode_scan_response(&body).map_err(KayaError::internal)?;
                    if json {
                        print!("{{\"items\":[");
                        for (i, (k, v)) in items.iter().enumerate() {
                            if i > 0 {
                                print!(",");
                            }
                            let ks = String::from_utf8_lossy(k);
                            let vs = String::from_utf8_lossy(v);
                            print!(
                                "{{\"key\":{},\"value\":{}}}",
                                json_string(&ks),
                                json_string(&vs)
                            );
                        }
                        println!("]}}");
                    } else {
                        for (k, v) in &items {
                            let ks = String::from_utf8_lossy(k);
                            let vs = String::from_utf8_lossy(v);
                            println!("{ks} {vs}");
                        }
                    }
                    Ok(())
                }
                STATUS_NOT_LEADER => Err(KayaError::internal(
                    "not leader — retry on a different node",
                )),
                STATUS_ERROR => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::internal(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "health" => {
            let (status, body) = roundtrip(addr, 5, &[])
                .await
                .map_err(|e| KayaError::internal(e.to_string()))?;
            if status == STATUS_OK {
                let role = String::from_utf8_lossy(&body);
                if json {
                    println!("{{\"ok\":true,\"role\":\"{role}\"}}");
                } else {
                    println!("OK role={role}");
                }
                Ok(())
            } else {
                Err(KayaError::internal("health check failed"))
            }
        }
        [cmd] if cmd == "status" => {
            let (status, body) = roundtrip_with_retry(addr, 6, &[])
                .await
                .map_err(|e| KayaError::internal(e.to_string()))?;
            if status == STATUS_OK {
                let stats_str =
                    String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
                if json {
                    println!("{}", stats_str);
                } else {
                    print_human_stats_from_json(&stats_str);
                }
                Ok(())
            } else {
                Err(KayaError::internal("status check failed"))
            }
        }
        _ => Err(KayaError::invalid_argument(
            "unknown command for --server mode",
        )),
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(future)
}

fn remove_flag(args: &mut Vec<String>, flag: &str) -> bool {
    let before = args.len();
    args.retain(|arg| arg != flag);
    args.len() != before
}

fn remove_value_flag(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let pos = args.iter().position(|arg| arg == flag)?;
    if pos + 1 < args.len() {
        args.remove(pos);
        Some(args.remove(pos))
    } else {
        None
    }
}

fn print_usage() {
    println!("kayactl — KayaDB command-line tool");
    println!();
    println!("LOCAL ENGINE COMMANDS");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] put <key> <value>");
    println!("  kayactl [--data <dir>] [--json] get <key>");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] delete <key>");
    println!("  kayactl [--data <dir>] [--json] scan <prefix>");
    println!();
    println!("OBSERVABILITY COMMANDS");
    println!("  kayactl [--data <dir>] [--json] stats");
    println!("  kayactl [--data <dir>] [--json] recover --dry-run");
    println!();
    println!("CLUSTER MODE (via running kayadb-server)");
    println!("  kayactl --server <addr> [--json] put <key> <value>");
    println!("  kayactl --server <addr> [--json] get <key>");
    println!("  kayactl --server <addr> [--json] delete <key>");
    println!("  kayactl --server <addr> [--json] scan <prefix>");
    println!("  kayactl --server <addr> [--json] health");
    println!("  kayactl --server <addr> [--json] status");
    println!();
    println!("INSPECT COMMANDS");
    println!("  kayactl [--json] inspect wal <path>");
    println!("  kayactl [--json] inspect sstable <path>");
    println!("  kayactl [--json] inspect manifest <path>");
    println!();
    println!("DEFAULTS");
    println!("  --data ./data");
    println!("  --durability strict");
}

fn print_human_stats_from_json(json: &str) {
    println!("=== KayaDB Cluster Node Status ===");
    let extract = |key: &str| -> Option<String> {
        let pattern = format!("\"{}\":", key);
        if let Some(pos) = json.find(&pattern) {
            let start = pos + pattern.len();
            let mut end = start;
            let bytes = json.as_bytes();
            let mut in_quotes = false;
            while end < bytes.len() {
                let c = bytes[end] as char;
                if c == '"' {
                    in_quotes = !in_quotes;
                } else if !in_quotes && (c == ',' || c == '}' || c == '{') {
                    break;
                }
                end += 1;
            }
            let val = &json[start..end];
            return Some(val.replace("\"", "").trim().to_string());
        }
        None
    };

    if let Some(role) = extract("role") {
        println!("Role:           {}", role);
    }
    if let Some(term) = extract("term") {
        println!("Term:           {}", term);
    }
    if let Some(commit) = extract("commit_index") {
        println!("Commit Index:   {}", commit);
    }
    if let Some(applied) = extract("applied_index") {
        println!("Applied Index:  {}", applied);
    }
    if let Some(peers) = extract("peer_count") {
        println!("Peer Count:     {}", peers);
    }

    println!("\n--- LSM Storage Engine Metrics ---");
    if let Some(put) = extract("put_count") {
        println!("PUT Operations:       {}", put);
    }
    if let Some(get) = extract("get_count") {
        println!("GET Operations:       {}", get);
    }
    if let Some(delete) = extract("delete_count") {
        println!("DELETE Operations:    {}", delete);
    }
    if let Some(scan) = extract("scan_count") {
        println!("SCAN Operations:      {}", scan);
    }
    if let Some(wal_bytes) = extract("wal_bytes_written") {
        println!("WAL Bytes Written:    {} bytes", wal_bytes);
    }
    if let Some(wal_fsync) = extract("wal_fsync_count") {
        println!("WAL Fsync Count:      {}", wal_fsync);
    }
    if let Some(mem_entries) = extract("memtable_entries") {
        println!("Memtable Entries:     {}", mem_entries);
    }
    if let Some(sst_count) = extract("sstable_count") {
        println!("SSTable Count:        {}", sst_count);
    }
    if let Some(last_seq) = extract("last_sequence") {
        println!("Last Sequence Number: {}", last_seq);
    }
    println!("==================================");
}

fn print_stats_human(stats: &EngineStats, recovery: &RecoveryReport) {
    println!("put_count:         {}", stats.put_count);
    println!("get_count:         {}", stats.get_count);
    println!("delete_count:      {}", stats.delete_count);
    println!("scan_count:        {}", stats.scan_count);
    println!("wal_bytes_written: {}", stats.wal_bytes_written);
    println!("wal_fsync_count:   {}", stats.wal_fsync_count);
    println!("memtable_entries:  {}", stats.memtable_entries);
    println!("sstable_count:     {}", stats.sstable_count);
    println!("last_sequence:     {}", stats.last_sequence);
    println!();
    println!("recovery.records_replayed: {}", recovery.records_replayed);
    println!(
        "recovery.wal_truncated_bytes: {}",
        recovery.wal.truncated_bytes
    );
    for w in &recovery.wal.warnings {
        println!("recovery.warning: {w}");
    }
}

fn print_stats_json(stats: &EngineStats, recovery: &RecoveryReport) {
    print!(
        "{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},",
        stats.put_count, stats.get_count, stats.delete_count, stats.scan_count
    );
    print!(
        "\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{},",
        stats.wal_bytes_written, stats.wal_fsync_count, stats.memtable_entries, stats.sstable_count, stats.last_sequence
    );
    print!(
        "\"recovery\":{{\"records_replayed\":{},\"wal_truncated_bytes\":{},\"warnings\":[",
        recovery.records_replayed, recovery.wal.truncated_bytes
    );
    for (i, w) in recovery.wal.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}}}");
}

fn print_recovery_human(recovery: &RecoveryReport) {
    println!("records_replayed:    {}", recovery.records_replayed);
    println!("wal_truncated_bytes: {}", recovery.wal.truncated_bytes);
    println!(
        "last_lsn:            {}",
        recovery.wal.last_lsn.map_or(0, |l| l.get())
    );
    for w in &recovery.wal.warnings {
        println!("warning: {w}");
    }
    if recovery.wal.warnings.is_empty() {
        println!("warnings:            none");
    }
}

fn print_recovery_json(recovery: &RecoveryReport) {
    print!(
        "{{\"records_replayed\":{},\"wal_truncated_bytes\":{},\"last_lsn\":{},\"warnings\":[",
        recovery.records_replayed,
        recovery.wal.truncated_bytes,
        recovery.wal.last_lsn.map_or(0, |l| l.get())
    );
    for (i, w) in recovery.wal.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}");
}

fn print_inspection_human(inspection: &WalInspection) {
    println!("segment: {}", inspection.segment);
    println!("records: {}", inspection.rows.len());
    println!();
    for row in &inspection.rows {
        match row.value_len {
            Some(value_len) => println!(
                "offset={} lsn={} seq={} type={} key_len={} value_len={} checksum=ok",
                row.offset,
                row.lsn,
                row.sequence,
                row.record_type,
                row.key_len.unwrap_or_default(),
                value_len
            ),
            None => println!(
                "offset={} lsn={} seq={} type={} key_len={} checksum=ok",
                row.offset,
                row.lsn,
                row.sequence,
                row.record_type,
                row.key_len.unwrap_or_default()
            ),
        }
    }
    for warning in &inspection.warnings {
        println!("CORRUPTION {warning}");
    }
}

fn print_inspection_json(inspection: &WalInspection) {
    print!(
        "{{\"segment\":\"{}\",\"records\":[",
        json_escape(&inspection.segment)
    );
    for (index, row) in inspection.rows.iter().enumerate() {
        if index > 0 {
            print!(",");
        }
        print!(
            "{{\"offset\":{},\"lsn\":{},\"sequence\":{},\"type\":\"{}\",\"key_len\":{}",
            row.offset,
            row.lsn,
            row.sequence,
            row.record_type,
            option_usize_json(row.key_len)
        );
        print!(",\"value_len\":{}}}", option_usize_json(row.value_len));
    }
    print!("],\"warnings\":[");
    for (index, warning) in inspection.warnings.iter().enumerate() {
        if index > 0 {
            print!(",");
        }
        print!("\"{}\"", json_escape(&warning.to_string()));
    }
    println!("]}}");
}

fn option_usize_json(value: Option<usize>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn json_string(value: &str) -> String {
    format!("\"{}\"", json_escape(value))
}

fn json_escape(value: &str) -> String {
    value
        .chars()
        .flat_map(|ch| match ch {
            '"' => "\\\"".chars().collect::<Vec<_>>(),
            '\\' => "\\\\".chars().collect::<Vec<_>>(),
            '\n' => "\\n".chars().collect::<Vec<_>>(),
            '\r' => "\\r".chars().collect::<Vec<_>>(),
            '\t' => "\\t".chars().collect::<Vec<_>>(),
            other => vec![other],
        })
        .collect()
}

fn print_sst_inspection_human(inspection: &SstInspection) {
    println!("sstable: {}", inspection.path);
    let f = &inspection.footer;
    println!(
        "version={} entries={} min_seq={} max_seq={}",
        f.format_version, f.entry_count, f.table_min_seq, f.table_max_seq
    );
    println!();
    for entry in &inspection.entries {
        let key = String::from_utf8_lossy(&entry.key);
        match &entry.value {
            Some(v) => {
                let value = String::from_utf8_lossy(v);
                println!(
                    "seq={} PUT key={key} value_len={}  {value}",
                    entry.sequence.get(),
                    v.len()
                );
            }
            None => {
                println!("seq={} DEL key={key}", entry.sequence.get());
            }
        }
    }
    for warning in &inspection.warnings {
        println!("WARNING {warning}");
    }
}

fn print_sst_inspection_json(inspection: &SstInspection) {
    let f = &inspection.footer;
    print!(
        "{{\"path\":{},\"version\":{},\"entry_count\":{},\"min_seq\":{},\"max_seq\":{},\"entries\":[",
        json_string(&inspection.path),
        f.format_version,
        f.entry_count,
        f.table_min_seq,
        f.table_max_seq
    );
    for (i, entry) in inspection.entries.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        let key = String::from_utf8_lossy(&entry.key);
        match &entry.value {
            Some(v) => {
                let value = String::from_utf8_lossy(v);
                print!(
                    "{{\"seq\":{},\"type\":\"put\",\"key\":{},\"value\":{}}}",
                    entry.sequence.get(),
                    json_string(&key),
                    json_string(&value)
                );
            }
            None => {
                print!(
                    "{{\"seq\":{},\"type\":\"del\",\"key\":{}}}",
                    entry.sequence.get(),
                    json_string(&key)
                );
            }
        }
    }
    print!("],\"warnings\":[");
    for (i, w) in inspection.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(w));
    }
    println!("]}}");
}

fn print_manifest_inspection_human(inspection: &ManifestInspection) {
    println!("manifest: {}", inspection.path);
    let s = &inspection.state;
    println!(
        "live_tables={} last_sequence={} last_edit_seq={}",
        s.live_tables.len(),
        s.last_sequence.get(),
        s.last_edit_seq
    );
    println!();
    for t in &s.live_tables {
        let sk = String::from_utf8_lossy(&t.smallest_key);
        let lk = String::from_utf8_lossy(&t.largest_key);
        println!(
            "table_id={} level={} entries={} path={} min_seq={} max_seq={} smallest={sk} largest={lk}",
            t.table_id, t.level, t.entry_count, t.path,
            t.min_sequence.get(), t.max_sequence.get()
        );
    }
    for warning in &inspection.warnings {
        println!("WARNING {warning}");
    }
}

fn print_manifest_inspection_json(inspection: &ManifestInspection) {
    let s = &inspection.state;
    print!(
        "{{\"path\":{},\"last_sequence\":{},\"last_edit_seq\":{},\"live_tables\":[",
        json_string(&inspection.path),
        s.last_sequence.get(),
        s.last_edit_seq
    );
    for (i, t) in s.live_tables.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        let sk = String::from_utf8_lossy(&t.smallest_key);
        let lk = String::from_utf8_lossy(&t.largest_key);
        print!(
            "{{\"table_id\":{},\"level\":{},\"path\":{},\"entries\":{},\"min_seq\":{},\"max_seq\":{},\"smallest\":{},\"largest\":{}}}",
            t.table_id,
            t.level,
            json_string(&t.path),
            t.entry_count,
            t.min_sequence.get(),
            t.max_sequence.get(),
            json_string(&sk),
            json_string(&lk)
        );
    }
    print!("],\"warnings\":[");
    for (i, w) in inspection.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(w));
    }
    println!("]}}");
}
