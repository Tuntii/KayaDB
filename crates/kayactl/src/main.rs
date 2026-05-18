use std::env;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use kaya_core::{DurabilityMode, EngineConfig, KayaError, Result, WalConfig};
use kaya_engine::{Engine, ReadOptions, ScanOptions, WriteOptions};
use kaya_io::FileDisk;
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
        _ => Err(KayaError::invalid_argument("unknown kayactl command")),
    }
}

async fn open_engine(
    data_dir: String,
    default_durability: DurabilityMode,
) -> Result<Engine<FileDisk>> {
    let mut config = EngineConfig::default();
    config.data_dir = PathBuf::from(&data_dir);
    config.durability.mode = default_durability;
    let disk = Arc::new(FileDisk::new(config.data_dir.clone()));
    Engine::open(config, disk).await
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
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
    println!("KEY-VALUE COMMANDS");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] put <key> <value>");
    println!("  kayactl [--data <dir>] [--json] get <key>");
    println!("  kayactl [--data <dir>] [--durability strict|relaxed] [--json] delete <key>");
    println!("  kayactl [--data <dir>] [--json] scan <prefix>");
    println!();
    println!("INSPECT COMMANDS");
    println!("  kayactl [--json] inspect wal <path>");
    println!();
    println!("DEFAULTS");
    println!("  --data ./data");
    println!("  --durability strict");
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
