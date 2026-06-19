use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use kaya_core::{DurabilityMode, EngineConfig, KayaError, Result, WalConfig};
use kaya_engine::{
    recover as engine_recover, Engine, EngineStats, ReadOptions, RecoveryReport, ScanOptions,
    WriteOptions,
};
use kaya_io::FileDisk;
use kaya_lsm::{inspect_manifest_path, inspect_sstable_path, ManifestInspection, SstInspection};
use kaya_net::{
    decode_error_payload, decode_scan_response, decode_value_payload, encode_admin_payload,
    encode_key_payload, encode_member_payload, encode_put_payload, encode_remove_member_payload,
    encode_scan_payload, roundtrip, ADD_MEMBER_OPCODE, REMOVE_MEMBER_OPCODE, STATUS_ERROR,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND, STATUS_NOT_LEADER, STATUS_OK,
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

    let operator_token = remove_value_flag(&mut args, "--operator-token")
        .or_else(|| env::var("KAYA_OPERATOR_TOKEN").ok())
        .filter(|t| !t.trim().is_empty());

    let _use_tls = remove_flag(&mut args, "--tls");
    let _tls_ca_cert = remove_value_flag(&mut args, "--tls-ca-cert");

    let server_addrs: Vec<SocketAddr> = remove_all_value_flags(&mut args, "--server")
        .into_iter()
        .map(|s| {
            s.parse::<SocketAddr>()
                .map_err(|e| KayaError::invalid_argument(format!("--server: {e}")))
        })
        .collect::<Result<Vec<_>>>()?;

    let timeout_ms: Option<u64> = remove_value_flag(&mut args, "--timeout")
        .map(|s| {
            s.parse::<u64>()
                .map_err(|e| KayaError::invalid_argument(format!("--timeout: {e}")))
        })
        .transpose()?;
    let timeout = timeout_ms.map(Duration::from_millis);

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

    let latency_view = remove_flag(&mut args, "--latency");

    // ── eBPF observability (Linux experiments, M12) ───────────────────────────
    // Handled early so it works standalone or alongside --server/--data.
    if !args.is_empty() && args[0] == "ebpf" {
        let sub = if args.len() > 1 {
            args[1].clone()
        } else {
            "help".to_string()
        };
        let pid: Option<u32> = remove_value_flag(&mut args, "--pid").and_then(|s| s.parse().ok());
        let run = remove_flag(&mut args, "--run");
        let duration: Option<String> = remove_value_flag(&mut args, "--duration");
        return handle_ebpf(&sub, pid, run, duration, json);
    }

    // ── server mode ───────────────────────────────────────────────────────────
    if !server_addrs.is_empty() {
        return run_server_mode(
            args,
            server_addrs,
            json,
            timeout,
            latency_view,
            operator_token,
        );
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
            } else if latency_view {
                print_latency_human(&stats);
            } else {
                print_stats_human(&stats, &recovery);
            }
            Ok(())
        }
        [cmd] if cmd == "flush" => {
            let (flush_res, stats) = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                let r = engine.flush().await?;
                let s = engine.stats();
                Ok::<_, KayaError>((r, s))
            })?;
            if json {
                println!(
                    "{{\"ok\":true,\"memtable_entries\":{},\"sstable_count\":{}}}",
                    flush_res.memtable_entries, flush_res.sstable_count
                );
            } else {
                println!(
                    "OK flushed {} memtable entries. Live SSTables: {}",
                    flush_res.memtable_entries, flush_res.sstable_count
                );
                println!();
                print_latency_human(&stats);
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
    endpoints: &[SocketAddr],
    opcode: u8,
    payload: &[u8],
    timeout: Option<Duration>,
) -> Result<(u16, Vec<u8>)> {
    let mut addr_idx = 0;
    let mut redirect_retries = 0;
    let mut current_addr = endpoints[0];

    loop {
        let request = roundtrip(current_addr, opcode, payload);
        let result = match timeout {
            Some(dur) => match tokio::time::timeout(dur, request).await {
                Ok(r) => r,
                Err(_) => {
                    return Err(KayaError::internal(format!(
                        "request to {current_addr} timed out after {}ms",
                        dur.as_millis()
                    )));
                }
            },
            None => request.await,
        };
        let (status, body) = result.map_err(|e| KayaError::internal(e.to_string()))?;

        if status == STATUS_NOT_LEADER && redirect_retries < 3 && !body.is_empty() {
            if let Ok(leader_addr_str) = String::from_utf8(body.clone()) {
                if let Ok(new_addr) = leader_addr_str.parse::<SocketAddr>() {
                    eprintln!("Redirecting to leader at {}...", new_addr);
                    current_addr = new_addr;
                    redirect_retries += 1;
                    continue;
                }
            }
        }

        // Single-node (or early election) case: the node replied NOT_LEADER with
        // no hint because it hasn't finished electing yet. Retry same target briefly.
        if status == STATUS_NOT_LEADER && redirect_retries < 6 {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            redirect_retries += 1;
            continue;
        }

        if status == STATUS_ERROR || status == STATUS_INVALID_ARGUMENT {
            if let Ok(msg) = decode_error_payload(&body) {
                if (msg.contains("connection") || msg.contains("unavailable"))
                    && addr_idx + 1 < endpoints.len()
                {
                    addr_idx += 1;
                    current_addr = endpoints[addr_idx];
                    eprintln!("Trying next endpoint: {current_addr}");
                    continue;
                }
            }
        }

        return Ok((status, body));
    }
}

fn run_server_mode(
    args: Vec<String>,
    endpoints: Vec<SocketAddr>,
    json: bool,
    timeout: Option<Duration>,
    latency_view: bool,
    operator_token: Option<String>,
) -> Result<()> {
    block_on(async move {
        run_server_mode_async(args, endpoints, json, timeout, latency_view, operator_token).await
    })
}

async fn run_server_mode_async(
    args: Vec<String>,
    endpoints: Vec<SocketAddr>,
    json: bool,
    timeout: Option<Duration>,
    latency_view: bool,
    operator_token: Option<String>,
) -> Result<()> {
    // operator_token already parsed from flag/env at top level (global)

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
            let (status, body) = roundtrip_with_retry(&endpoints, 1, &payload, timeout).await?;
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
                STATUS_ERROR | STATUS_INVALID_ARGUMENT => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::invalid_argument(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "get" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> get <key>",
        )),
        [cmd, key] if cmd == "get" => {
            let payload = encode_key_payload(key.as_bytes());
            let (status, body) = roundtrip_with_retry(&endpoints, 2, &payload, timeout).await?;
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
                STATUS_ERROR | STATUS_INVALID_ARGUMENT => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::invalid_argument(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "delete" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> delete <key>",
        )),
        [cmd, key] if cmd == "delete" => {
            let payload = encode_key_payload(key.as_bytes());
            let (status, body) = roundtrip_with_retry(&endpoints, 3, &payload, timeout).await?;
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
                STATUS_ERROR | STATUS_INVALID_ARGUMENT => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::invalid_argument(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "scan" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> scan <prefix>",
        )),
        [cmd, prefix] if cmd == "scan" => {
            let payload = encode_scan_payload(prefix.as_bytes());
            let (status, body) = roundtrip_with_retry(&endpoints, 4, &payload, timeout).await?;
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
                STATUS_ERROR | STATUS_INVALID_ARGUMENT => {
                    let msg = decode_error_payload(&body).unwrap_or_else(|_| "unknown".into());
                    Err(KayaError::invalid_argument(msg))
                }
                s => Err(KayaError::internal(format!("unexpected status: {s}"))),
            }
        }
        [cmd] if cmd == "health" => {
            let (status, body) = roundtrip_with_retry(&endpoints, 5, &[], timeout).await?;
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
            let (status, body) = roundtrip_with_retry(&endpoints, 6, &[], timeout).await?;
            if status == STATUS_OK {
                let stats_str =
                    String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
                if json {
                    println!("{}", stats_str);
                } else if latency_view {
                    // Best-effort: the full human already includes latency fields now.
                    // For a focused view we still go through the extractor (it prints everything relevant).
                    print_human_stats_from_json(&stats_str);
                    // Future: could parse and call a pure latency printer, but enriched human is good.
                } else {
                    print_human_stats_from_json(&stats_str);
                }
                Ok(())
            } else {
                Err(KayaError::internal("status check failed"))
            }
        }
        [cmd] if cmd == "add-node" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> [--operator-token <tok>] add-node <id> <raft-addr> <client-addr>",
        )),
        [cmd, id, raft_addr, client_addr] if cmd == "add-node" => {
            let node_id: u64 = id
                .parse()
                .map_err(|e| KayaError::invalid_argument(format!("node id: {e}")))?;
            let inner = encode_member_payload(node_id, raft_addr, client_addr);
            let payload = match &operator_token {
                Some(tok) => encode_admin_payload(ADD_MEMBER_OPCODE, &inner, Some(tok.as_str())),
                None => inner,
            };
            let (status, body) = roundtrip_with_retry(&endpoints, ADD_MEMBER_OPCODE, &payload, timeout).await?;
            handle_membership_response(status, &body, json, "add-node")
        }
        [cmd] if cmd == "remove-node" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> [--operator-token <tok>] remove-node <id>",
        )),
        [cmd, id] if cmd == "remove-node" => {
            let node_id: u64 = id
                .parse()
                .map_err(|e| KayaError::invalid_argument(format!("node id: {e}")))?;
            let inner = encode_remove_member_payload(node_id);
            let payload = match &operator_token {
                Some(tok) => encode_admin_payload(REMOVE_MEMBER_OPCODE, &inner, Some(tok.as_str())),
                None => inner,
            };
            let (status, body) = roundtrip_with_retry(&endpoints, REMOVE_MEMBER_OPCODE, &payload, timeout).await?;
            handle_membership_response(status, &body, json, "remove-node")
        }
        _ => Err(KayaError::invalid_argument(
            "unknown command for --server mode",
        )),
    }
}

fn handle_membership_response(status: u16, body: &[u8], json: bool, op: &str) -> Result<()> {
    match status {
        STATUS_OK => {
            let msg = String::from_utf8_lossy(body);
            if json {
                println!(
                    "{{\"ok\":true,\"op\":\"{op}\",\"message\":{}}}",
                    json_string(&msg)
                );
            } else {
                println!("OK {msg}");
            }
            Ok(())
        }
        STATUS_NOT_LEADER => Err(KayaError::internal(
            "not leader — retry on a different node",
        )),
        STATUS_ERROR | STATUS_INVALID_ARGUMENT => {
            let msg = decode_error_payload(body).unwrap_or_else(|_| "unknown".into());
            Err(KayaError::invalid_argument(msg))
        }
        s => Err(KayaError::internal(format!("unexpected status: {s}"))),
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

fn remove_all_value_flags(args: &mut Vec<String>, flag: &str) -> Vec<String> {
    let mut values = Vec::new();
    while let Some(v) = remove_value_flag(args, flag) {
        values.push(v);
    }
    values
}

fn print_usage() {
    println!("kayactl — KayaDB command-line tool");
    println!();
    println!("LOCAL ENGINE COMMANDS");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] put <key> <value>");
    println!("  kayactl [--data <dir>] [--json] get <key>");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] delete <key>");
    println!("  kayactl [--data <dir>] [--json] scan <prefix>");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] flush   (force memtable -> SSTable for observability)");
    println!();
    println!("OBSERVABILITY COMMANDS");
    println!("  kayactl [--data <dir>] [--json] [--latency] stats   (add --latency for focused durability + flush/compaction timers)");
    println!("  kayactl [--data <dir>] [--durability ...] [--json] flush   (force publish to see latency numbers move; pairs with --latency and ebpf probes)");
    println!("  kayactl [--data <dir>] [--json] recover --dry-run");
    println!("  kayactl ebpf [fsync-latency|...|list|status|help] [--pid <pid>] [--run] [--duration 30s]   (Linux eBPF experiments, Track A)");
    println!();
    println!("CLUSTER MODE (via running kayadb-server)");
    println!("  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] put <key> <value>");
    println!(
        "  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] get <key>"
    );
    println!(
        "  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] delete <key>"
    );
    println!(
        "  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] scan <prefix>"
    );
    println!("  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] health");
    println!("  kayactl --server <addr> [--server <addr2> ...] [--timeout <ms>] [--operator-token <tok>] [--json] [--latency] status");
    println!("  kayactl --server <addr> [--operator-token <tok>] add-node <id> <raft-addr> <client-addr>");
    println!("  kayactl --server <addr> [--operator-token <tok>] remove-node <id>");
    println!();
    println!("INSPECT COMMANDS");
    println!("  kayactl [--json] inspect wal <path>");
    println!("  kayactl [--json] inspect sstable <path>");
    println!("  kayactl [--json] inspect manifest <path>");
    println!();
    println!("DEFAULTS");
    println!("  --data ./data");
    println!("  --durability strict");
    println!("  --timeout (none)");
    println!("  --operator-token (none, or KAYA_OPERATOR_TOKEN env var)");
}

/// Handle `kayactl ebpf ...` subcommands.
/// Linux eBPF observability experiments (Track A / M12). Non-Linux and missing tools are handled gracefully.
/// If `run` is true we attempt to exec bpftrace (requires appropriate privileges on Linux).
/// `duration` (e.g. "30s") is best-effort for --run (uses `timeout` on Unix if available).
fn handle_ebpf(
    sub: &str,
    explicit_pid: Option<u32>,
    run: bool,
    duration: Option<String>,
    _json: bool,
) -> Result<()> {
    const LINUX_ONLY_MSG: &str = "eBPF probes are Linux-only. This command provides ready-to-run bpftrace commands and guidance (see scripts/ebpf/).";

    if !cfg!(target_os = "linux") {
        println!("{}", LINUX_ONLY_MSG);
        println!(
            "Scripts live in scripts/ebpf/ and work on any Linux machine with bpftrace + sudo."
        );
        return Ok(());
    }

    // Try to auto-detect a KayaDB server PID if none supplied.
    let pid = explicit_pid.or_else(|| {
        // Best-effort: look for kayadb-server first, then kayactl (for local engine tests)
        std::process::Command::new("pgrep")
            .args(["-f", "kayadb-server"])
            .output()
            .ok()
            .and_then(|o| {
                String::from_utf8(o.stdout)
                    .ok()
                    .and_then(|s| s.lines().next().and_then(|l| l.trim().parse::<u32>().ok()))
            })
            .or_else(|| {
                std::process::Command::new("pgrep")
                    .args(["-f", "kayactl"])
                    .output()
                    .ok()
                    .and_then(|o| {
                        String::from_utf8(o.stdout).ok().and_then(|s| {
                            s.lines().next().and_then(|l| l.trim().parse::<u32>().ok())
                        })
                    })
            })
    });

    let pid_str = pid
        .map(|p| p.to_string())
        .unwrap_or_else(|| "<PID>".to_string());
    let pid_hint = if pid.is_some() {
        format!("(auto-detected PID {})", pid_str)
    } else {
        "(pass --pid <N> or run against a live kayadb-server)".to_string()
    };

    match sub {
        "fsync-latency" | "fsync" => {
            println!("KayaDB eBPF: fsync / fdatasync latency (microseconds)");
            println!("PID hint: {}", pid_hint);
            println!();
            println!("Recommended (copy-paste):");
            println!(
                "  sudo bpftrace -p {} scripts/ebpf/fsync-latency.bt",
                pid_str
            );
            println!();
            println!("Alternative one-liner:");
            println!("  sudo bpftrace -e '");
            println!("    kprobe:sys_fsync, kprobe:sys_fdatasync {{ @start[tid] = nsecs; }}");
            println!("    kretprobe:sys_fsync, kretprobe:sys_fdatasync /@start[tid]/ {{");
            println!("      $us = (nsecs - @start[tid]) / 1000;");
            println!("      @fsync_us = hist($us); delete(@start[tid]);");
            println!("    }}' -p {}", pid_str);
            println!();
            println!("See: scripts/ebpf/README.md and spec/docs/observability-spec.md");
            // Attempt to run if bpftrace exists and we have a real PID (best effort, user will see permission errors)
            if pid.is_some()
                && std::process::Command::new("bpftrace")
                    .arg("--version")
                    .output()
                    .is_ok()
            {
                println!("\n[bpftrace found] You can now run the sudo command above.");
            }
            if run {
                return try_run_bpftrace("fsync-latency", &pid_str, duration.as_deref());
            }
            Ok(())
        }
        "block-latency" | "bio" | "block" => {
            println!("KayaDB eBPF: block I/O (device) latency histograms (us)");
            println!("PID hint: {}", pid_hint);
            println!();
            println!("Recommended (copy-paste):");
            println!(
                "  sudo bpftrace -p {} scripts/ebpf/block-io-latency.bt",
                pid_str
            );
            println!();
            println!("This shows time spent in the storage stack / scheduler / device after the fsync syscall.");
            println!("See: scripts/ebpf/README.md");
            if pid.is_some()
                && std::process::Command::new("bpftrace")
                    .arg("--version")
                    .output()
                    .is_ok()
            {
                println!("\n[bpftrace found] Ready to attach.");
            }
            if run {
                return try_run_bpftrace("block-io-latency", &pid_str, duration.as_deref());
            }
            Ok(())
        }
        "syscall-timeline" | "timeline" | "sys" => {
            println!("KayaDB eBPF: syscall timeline (write, fsync, fdatasync, rename, unlink, fsyncdir) + TID correlation");
            println!("PID hint: {}", pid_hint);
            println!();
            println!("Recommended (copy-paste):");
            println!(
                "  sudo bpftrace -p {} scripts/ebpf/syscall-timeline.bt",
                pid_str
            );
            println!();
            println!("This script correlates writes with their fsyncs by TID and shows rename/unlink for flush/compaction publish points.");
            println!("See: scripts/ebpf/README.md (Track A addition)");
            if pid.is_some()
                && std::process::Command::new("bpftrace")
                    .arg("--version")
                    .output()
                    .is_ok()
            {
                println!("\n[bpftrace found] Ready to attach.");
            }
            if run {
                return try_run_bpftrace("syscall-timeline", &pid_str, duration.as_deref());
            }
            Ok(())
        }
        "list" => {
            println!("kayactl ebpf list — active KayaDB / bpftrace processes (best effort)");
            if !cfg!(target_os = "linux") {
                println!("{}", LINUX_ONLY_MSG);
                return Ok(());
            }
            // List kayadb-server instances (cluster friendly)
            println!("\n--- kayadb-server processes ---");
            if let Ok(out) = std::process::Command::new("pgrep")
                .args(["-a", "kayadb-server"])
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.trim().is_empty() {
                    println!("  (none found)");
                } else {
                    for line in s.lines() {
                        println!("  {}", line.trim());
                    }
                }
            } else {
                println!("  pgrep unavailable");
            }
            // List kayactl (embedded tests)
            println!("\n--- kayactl processes ---");
            if let Ok(out) = std::process::Command::new("pgrep")
                .args(["-a", "kayactl"])
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.trim().is_empty() {
                    println!("  (none found)");
                } else {
                    for line in s.lines() {
                        println!("  {}", line.trim());
                    }
                }
            }
            // List running bpftrace traces (useful for "status" of traces)
            println!("\n--- bpftrace / trace processes ---");
            if let Ok(out) = std::process::Command::new("pgrep")
                .args(["-a", "bpftrace"])
                .output()
            {
                let s = String::from_utf8_lossy(&out.stdout);
                if s.trim().is_empty() {
                    println!("  (none found — no active traces)");
                } else {
                    for line in s.lines() {
                        println!("  {}", line.trim());
                    }
                }
            } else {
                println!("  pgrep bpftrace unavailable");
            }
            println!("\nUse the printed PIDs with --pid or the auto-detector.");
            Ok(())
        }
        "status" => {
            println!("kayactl ebpf status — quick view of local nodes + any attached traces");
            if !cfg!(target_os = "linux") {
                println!("{}", LINUX_ONLY_MSG);
                return Ok(());
            }
            // Reuse list logic but summarized
            let servers: Vec<String> = std::process::Command::new("pgrep")
                .args(["-f", "kayadb-server"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| {
                    s.lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(|l| l.trim().to_string())
                        .collect()
                })
                .unwrap_or_default();
            println!(
                "Detected kayadb-server PIDs: {}",
                if servers.is_empty() {
                    "(none)".into()
                } else {
                    servers.join(", ")
                }
            );
            let traces: Vec<String> = std::process::Command::new("pgrep")
                .args(["-a", "bpftrace"])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .map(|s| {
                    s.lines()
                        .filter(|l| !l.trim().is_empty())
                        .map(|l| l.trim().to_string())
                        .collect()
                })
                .unwrap_or_default();
            println!(
                "Active bpftrace traces: {}",
                if traces.is_empty() {
                    "0 (no kernel probes attached)".into()
                } else {
                    traces.len().to_string()
                }
            );
            println!("Tip: kayactl ebpf list   for full details.");
            Ok(())
        }
        _ => {
            println!("kayactl ebpf — Linux eBPF observability experiments (Track A / M12)");
            println!();
            println!("Subcommands:");
            println!(
                "  kayactl ebpf fsync-latency [--pid <pid>] [--run]        Trace fsync/fdatasync latency (us)"
            );
            println!(
                "  kayactl ebpf block-latency  [--pid <pid>] [--run]        Trace block device I/O latency (us)"
            );
            println!(
                "  kayactl ebpf syscall-timeline [--pid <pid>] [--run]    Trace writes + fsync/rename etc + simple TID correlation (new)"
            );
            println!("  kayactl ebpf list                                    List local kayadb-server + active bpftrace processes (multi-node friendly)");
            println!("  kayactl ebpf status                                  Summary of nodes + attached traces");
            println!("  kayactl ebpf help");
            println!();
            println!("These commands print ready-to-run bpftrace invocations. Use --run [--duration 30s] to try executing directly (you will likely need sudo).");
            println!("--run improvements: best-effort duration limit via `timeout` (Unix), output shown to terminal; for clusters run multiple in separate terminals or use external timeout.");
            println!("Auto-detect + 'list'/'status' find all kayadb-server PIDs (good for 3-node local clusters).");
            println!("Tip for Track A: `kayactl --data ... flush` (or repeated puts) + `stats --latency` generates visible activity for the probes.");
            println!("They require a Linux host with bpftrace installed and sufficient capabilities (usually sudo).");
            println!();
            println!("Full documentation: scripts/ebpf/README.md");
            println!("Observability spec:   spec/docs/observability-spec.md (section 7)");
            println!("Roadmap:              ROADMAP.md (Track A)");
            if !cfg!(target_os = "linux") {
                println!("\n{}", LINUX_ONLY_MSG);
            } else if pid.is_none() {
                println!("\nTip: start a server first (kayadb-server or scripts/start-cluster.sh), then re-run. Use 'ebpf list' to discover PIDs.");
            }
            Ok(())
        }
    }
}

/// Try to exec the named bpftrace script (best-effort, for developer convenience).
/// On Linux + bpftrace present this will (optionally with duration) run the trace.
/// The user is responsible for privileges (the script will usually need sudo or
/// CAP_BPF+CAP_PERFMON). We deliberately do *not* auto-sudo here.
/// duration: optional e.g. "30s" — on Unix we prefix with `timeout <duration>` if the binary exists.
fn try_run_bpftrace(kind: &str, pid_str: &str, duration: Option<&str>) -> Result<()> {
    if !cfg!(target_os = "linux") {
        eprintln!("--run is only supported on Linux.");
        return Ok(());
    }

    let script = match kind {
        "fsync-latency" => "scripts/ebpf/fsync-latency.bt",
        "block-io-latency" => "scripts/ebpf/block-io-latency.bt",
        "syscall-timeline" => "scripts/ebpf/syscall-timeline.bt",
        _ => {
            eprintln!("unknown eBPF script kind");
            return Ok(());
        }
    };

    if pid_str == "<PID>" {
        eprintln!("Cannot --run without a concrete PID. Use --pid N or ensure a kayadb-server is running for auto-detect.");
        return Ok(());
    }

    // Check bpftrace exists
    if std::process::Command::new("bpftrace")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("bpftrace not found in PATH. Install it first (see scripts/ebpf/README.md).");
        return Ok(());
    }

    eprintln!("WARNING: launching bpftrace for PID {}.", pid_str);
    if let Some(d) = duration {
        eprintln!("Duration limited to {} (best effort via timeout).", d);
    }
    eprintln!("This may require elevated privileges. If it fails with permission errors, re-run the printed sudo command manually.");
    eprintln!("Press Ctrl-C to stop the trace.\n");

    let mut cmd = std::process::Command::new("bpftrace");
    if let Some(d) = duration {
        // Best effort: use timeout(1) when available (common on Linux). Fall back to plain run.
        if std::process::Command::new("timeout")
            .arg("--version")
            .output()
            .is_ok()
        {
            cmd.arg(d); // timeout <duration> bpftrace ...
        } else {
            eprintln!("Note: 'timeout' command not found; running without duration limit.");
        }
    }
    let status = cmd.args(["-p", pid_str, script]).status();

    match status {
        Ok(s) => {
            if !s.success() {
                eprintln!("bpftrace exited with status: {}", s);
            }
        }
        Err(e) => {
            eprintln!("Failed to spawn bpftrace: {}", e);
        }
    }
    Ok(())
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
    if let Some(total_us) = extract("wal_fsync_total_us") {
        println!("WAL Fsync Total Us:   {}", total_us);
    }
    if let Some(max_us) = extract("wal_fsync_max_us") {
        println!("WAL Fsync Max Us:     {}", max_us);
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
    if let Some(f_cnt) = extract("flush_count") {
        println!("Flush Count:            {}", f_cnt);
    }
    if let Some(f_tot) = extract("flush_total_us") {
        println!("Flush Total Us:         {}", f_tot);
    }
    if let Some(f_max) = extract("flush_max_us") {
        println!("Flush Max Us:           {}", f_max);
    }
    if let Some(f_avg) = extract("flush_avg_us") {
        println!("Flush Avg Us:           {}", f_avg);
    }
    if let Some(c_cnt) = extract("compaction_count") {
        println!("Compaction Count:       {}", c_cnt);
    }
    if let Some(c_tot) = extract("compaction_total_us") {
        println!("Compaction Total Us:    {}", c_tot);
    }
    if let Some(c_max) = extract("compaction_max_us") {
        println!("Compaction Max Us:      {}", c_max);
    }
    if let Some(c_avg) = extract("compaction_avg_us") {
        println!("Compaction Avg Us:      {}", c_avg);
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
    println!("wal_fsync_total_us:{}", stats.wal_fsync_total_us);
    println!("wal_fsync_max_us:  {}", stats.wal_fsync_max_us);
    if let Some(avg) = stats.wal_fsync_total_us.checked_div(stats.wal_fsync_count) {
        println!("wal_fsync_avg_us:  {} (total/count)", avg);
    }
    println!("memtable_entries:  {}", stats.memtable_entries);
    println!("sstable_count:     {}", stats.sstable_count);
    println!("last_sequence:     {}", stats.last_sequence);
    // Track A latency metrics
    println!("flush_count:       {}", stats.flush_count);
    println!("flush_total_us:    {}", stats.flush_total_us);
    println!("flush_max_us:      {}", stats.flush_max_us);
    if let Some(avg) = stats.flush_total_us.checked_div(stats.flush_count) {
        println!("flush_avg_us:      {} (total/count)", avg);
    }
    println!("compaction_count:  {}", stats.compaction_count);
    println!("compaction_total_us: {}", stats.compaction_total_us);
    println!("compaction_max_us:   {}", stats.compaction_max_us);
    if let Some(avg) = stats
        .compaction_total_us
        .checked_div(stats.compaction_count)
    {
        println!("compaction_avg_us: {} (total/count)", avg);
    }
    println!();
    println!(
        "recovery.manifest_records_replayed: {}",
        recovery.manifest_records_replayed
    );
    println!(
        "recovery.live_sstable_count:        {}",
        recovery.live_sstable_count
    );
    println!(
        "recovery.wal_records_replayed:      {}",
        recovery.wal_records_replayed
    );
    println!(
        "recovery.wal_truncated_bytes:       {}",
        recovery.wal_truncated_bytes
    );
    println!(
        "recovery.tmp_files_removed:         {}",
        recovery.tmp_files_removed
    );
    println!(
        "recovery.last_lsn:                  {}",
        recovery.last_lsn.map_or(0, |l| l.get())
    );
    println!(
        "recovery.last_sequence:             {}",
        recovery.last_sequence.map_or(0, |s| s.get())
    );
    println!(
        "recovery.records_replayed:          {}",
        recovery.records_replayed
    );
    for w in &recovery.warnings {
        println!("recovery.warning: {w}");
    }
}

fn print_stats_json(stats: &EngineStats, recovery: &RecoveryReport) {
    print!(
        "{{\"put_count\":{},\"get_count\":{},\"delete_count\":{},\"scan_count\":{},",
        stats.put_count, stats.get_count, stats.delete_count, stats.scan_count
    );
    print!(
        "\"wal_bytes_written\":{},\"wal_fsync_count\":{},\"wal_fsync_total_us\":{},\"wal_fsync_max_us\":{},\"memtable_entries\":{},\"sstable_count\":{},\"last_sequence\":{},\"flush_total_us\":{},\"flush_max_us\":{},\"flush_count\":{},\"compaction_total_us\":{},\"compaction_max_us\":{},\"compaction_count\":{},",
        stats.wal_bytes_written, stats.wal_fsync_count, stats.wal_fsync_total_us, stats.wal_fsync_max_us, stats.memtable_entries, stats.sstable_count, stats.last_sequence,
        stats.flush_total_us, stats.flush_max_us, stats.flush_count,
        stats.compaction_total_us, stats.compaction_max_us, stats.compaction_count
    );
    print!(
        "\"recovery\":{{\"manifest_records_replayed\":{},\"live_sstable_count\":{},\"wal_records_replayed\":{},\"wal_truncated_bytes\":{},\"tmp_files_removed\":{},\"last_lsn\":{},\"last_sequence\":{},\"records_replayed\":{},\"warnings\":[",
        recovery.manifest_records_replayed,
        recovery.live_sstable_count,
        recovery.wal_records_replayed,
        recovery.wal_truncated_bytes,
        recovery.tmp_files_removed,
        recovery.last_lsn.map_or(0, |l| l.get()),
        recovery.last_sequence.map_or(0, |s| s.get()),
        recovery.records_replayed
    );
    for (i, w) in recovery.warnings.iter().enumerate() {
        if i > 0 {
            print!(",");
        }
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}}}");
}

/// Focused latency view (Track A). Use with `kayactl [--data] stats --latency` or pipe server status JSON.
fn print_latency_human(stats: &EngineStats) {
    println!("=== KayaDB Latency / Durability (Track A) ===");
    println!("WAL fsyncs (strict durability cost):");
    println!("  count:   {}", stats.wal_fsync_count);
    println!("  total_us:{}", stats.wal_fsync_total_us);
    println!("  max_us:  {}", stats.wal_fsync_max_us);
    if let Some(avg) = stats.wal_fsync_total_us.checked_div(stats.wal_fsync_count) {
        println!("  avg_us:  {avg}");
    }
    println!();
    println!("Flush (memtable -> SSTable publish, manifest, dir fsyncs):");
    println!("  count:   {}", stats.flush_count);
    println!("  total_us:{}", stats.flush_total_us);
    println!("  max_us:  {}", stats.flush_max_us);
    if let Some(avg) = stats.flush_total_us.checked_div(stats.flush_count) {
        println!("  avg_us:  {avg}");
    }
    println!();
    println!("Compaction (L0 merge + publish):");
    println!("  count:   {}", stats.compaction_count);
    println!("  total_us:{}", stats.compaction_total_us);
    println!("  max_us:  {}", stats.compaction_max_us);
    if let Some(avg) = stats
        .compaction_total_us
        .checked_div(stats.compaction_count)
    {
        println!("  avg_us:  {avg}");
    }
    println!();
    println!("Cross-reference with:");
    println!("  kayactl ebpf fsync-latency   (kernel fsync hist)");
    println!("  kayactl ebpf syscall-timeline (write/fsync/rename timeline + TID corr)");
    println!("  (Linux only; see scripts/ebpf/README.md)");
    println!("========================================");
}

fn print_recovery_human(recovery: &RecoveryReport) {
    println!(
        "manifest_records_replayed: {}",
        recovery.manifest_records_replayed
    );
    println!("live_sstable_count:        {}", recovery.live_sstable_count);
    println!(
        "wal_records_replayed:      {}",
        recovery.wal_records_replayed
    );
    println!(
        "wal_truncated_bytes:       {}",
        recovery.wal_truncated_bytes
    );
    println!("tmp_files_removed:         {}", recovery.tmp_files_removed);
    println!(
        "last_lsn:                  {}",
        recovery.last_lsn.map_or(0, |l| l.get())
    );
    println!(
        "last_sequence:             {}",
        recovery.last_sequence.map_or(0, |s| s.get())
    );
    println!("records_replayed:          {}", recovery.records_replayed);
    for w in &recovery.warnings {
        println!("warning: {w}");
    }
    if recovery.warnings.is_empty() {
        println!("warnings:            none");
    }
}

fn print_recovery_json(recovery: &RecoveryReport) {
    print!(
        "{{\"manifest_records_replayed\":{},\"live_sstable_count\":{},\"wal_records_replayed\":{},\"wal_truncated_bytes\":{},\"tmp_files_removed\":{},\"last_lsn\":{},\"last_sequence\":{},\"records_replayed\":{},\"warnings\":[",
        recovery.manifest_records_replayed,
        recovery.live_sstable_count,
        recovery.wal_records_replayed,
        recovery.wal_truncated_bytes,
        recovery.tmp_files_removed,
        recovery.last_lsn.map_or(0, |l| l.get()),
        recovery.last_sequence.map_or(0, |s| s.get()),
        recovery.records_replayed
    );
    for (i, w) in recovery.warnings.iter().enumerate() {
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
        print!("{}", json_string(&w.to_string()));
    }
    println!("]}}");
}
