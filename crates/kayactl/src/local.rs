use std::path::PathBuf;
use std::sync::Arc;

use kaya_core::{DurabilityConfig, DurabilityMode, EngineConfig, KayaError, Result};
use kaya_engine::{Engine, ReadOptions, ScanOptions, WriteOptions};
use kaya_io::FileDisk;

use crate::cli::{block_on, json_string, print_usage};
use crate::inspect;
use crate::stats_cmd;

pub(crate) async fn open_engine(
    data_dir: String,
    default_durability: DurabilityMode,
) -> Result<Engine<FileDisk>> {
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

pub(crate) fn run_local_mode(
    args: Vec<String>,
    data_dir: String,
    durability: DurabilityMode,
    json: bool,
    latency_view: bool,
) -> Result<()> {
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
        [inspect_cmd, wal, path] if inspect_cmd == "inspect" && wal == "wal" => {
            inspect::inspect_wal(path, json)
        }
        [inspect_cmd, sst, path] if inspect_cmd == "inspect" && sst == "sstable" => {
            inspect::inspect_sstable(path, json)
        }
        [inspect_cmd, mani, path] if inspect_cmd == "inspect" && mani == "manifest" => {
            inspect::inspect_manifest(path, json)
        }
        [cmd] if cmd == "stats" => {
            stats_cmd::run_local_stats(data_dir, durability, json, latency_view)
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
                stats_cmd::print_latency_human(&stats);
            }
            Ok(())
        }
        [cmd, flag] if cmd == "recover" && flag == "--dry-run" => {
            stats_cmd::run_recover_dry_run(data_dir, durability, json)
        }
        _ => Err(KayaError::invalid_argument("unknown kayactl command")),
    }
}
