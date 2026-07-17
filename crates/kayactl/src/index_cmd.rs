//! `kayactl index` — create / list / drop / scan / verify / backfill control.

use kaya_core::{DurabilityMode, KayaError, Result};
use kaya_engine::{
    BackfillMode, BackfillStatus, CreateIndexOptions, IndexDivergenceKind, IndexExtractor,
};

use crate::cli::{block_on, json_string, remove_flag, remove_value_flag};
use crate::local::open_engine;

fn parse_extractor(args: &mut Vec<String>) -> Result<IndexExtractor> {
    let kind = remove_value_flag(args, "--extractor").unwrap_or_else(|| "whole".to_owned());
    match kind.as_str() {
        "whole" | "value" => Ok(IndexExtractor::WholeValue),
        "prefix" => {
            let len: u16 = remove_value_flag(args, "--prefix-len")
                .ok_or_else(|| {
                    KayaError::invalid_argument("--extractor prefix requires --prefix-len <n>")
                })?
                .parse()
                .map_err(|e| KayaError::invalid_argument(format!("--prefix-len: {e}")))?;
            Ok(IndexExtractor::Prefix { len })
        }
        "field" => {
            let delim_s = remove_value_flag(args, "--delimiter").unwrap_or_else(|| "|".to_owned());
            let delimiter = if delim_s.len() == 1 {
                delim_s.as_bytes()[0]
            } else if delim_s == "comma" {
                b','
            } else if delim_s == "pipe" {
                b'|'
            } else {
                return Err(KayaError::invalid_argument(
                    "--delimiter must be a single byte, 'comma', or 'pipe'",
                ));
            };
            let index: u16 = remove_value_flag(args, "--field-index")
                .ok_or_else(|| {
                    KayaError::invalid_argument("--extractor field requires --field-index <n>")
                })?
                .parse()
                .map_err(|e| KayaError::invalid_argument(format!("--field-index: {e}")))?;
            Ok(IndexExtractor::Field { delimiter, index })
        }
        other => Err(KayaError::invalid_argument(format!(
            "unknown --extractor {other:?}; expected whole|prefix|field"
        ))),
    }
}

fn status_str(s: BackfillStatus) -> &'static str {
    match s {
        BackfillStatus::Idle => "idle",
        BackfillStatus::Running => "running",
        BackfillStatus::Paused => "paused",
        BackfillStatus::Complete => "complete",
    }
}

/// Entry: `args` starts with subcommand after `index` (or includes `index`).
pub fn run_index(
    mut args: Vec<String>,
    data_dir: String,
    durability: DurabilityMode,
    json: bool,
) -> Result<()> {
    if args.first().map(String::as_str) == Some("index") {
        args.remove(0);
    }
    let sub = args
        .first()
        .cloned()
        .ok_or_else(|| KayaError::invalid_argument(index_usage()))?;
    args.remove(0);

    match sub.as_str() {
        "create" => {
            let online = remove_flag(&mut args, "--online");
            let extractor = parse_extractor(&mut args)?;
            let name = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument(
                    "usage: kayactl index create <name> <primary_prefix> [--online] [--extractor ...]",
                )
            })?;
            let prefix = args.get(1).cloned().ok_or_else(|| {
                KayaError::invalid_argument(
                    "usage: kayactl index create <name> <primary_prefix> [--online] [--extractor ...]",
                )
            })?;
            let opts = CreateIndexOptions {
                extractor,
                backfill: if online {
                    BackfillMode::Online
                } else {
                    BackfillMode::Sync
                },
            };
            block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine
                    .create_index_with(&name, prefix.as_bytes(), opts)
                    .await
            })?;
            if json {
                println!(
                    "{{\"ok\":true,\"name\":{},\"online\":{}}}",
                    json_string(&name),
                    online
                );
            } else {
                println!(
                    "OK index {name} created ({})",
                    if online { "online backfill" } else { "sync backfill" }
                );
            }
            Ok(())
        }
        "list" => {
            let names = block_on(async {
                let engine = open_engine(data_dir, durability).await?;
                Ok::<_, KayaError>(engine.list_indexes())
            })?;
            if json {
                print!("{{\"indexes\":[");
                for (i, n) in names.iter().enumerate() {
                    if i > 0 {
                        print!(",");
                    }
                    print!("{}", json_string(n));
                }
                println!("]}}");
            } else if names.is_empty() {
                println!("(no indexes)");
            } else {
                for n in names {
                    println!("{n}");
                }
            }
            Ok(())
        }
        "drop" => {
            let name = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument("usage: kayactl index drop <name>")
            })?;
            block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine.drop_index(&name).await
            })?;
            if json {
                println!("{{\"ok\":true,\"dropped\":{}}}", json_string(&name));
            } else {
                println!("OK dropped index {name}");
            }
            Ok(())
        }
        "scan" => {
            let name = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument("usage: kayactl index scan <name> [value_prefix]")
            })?;
            let value_prefix = args.get(1).map(|s| s.as_bytes().to_vec()).unwrap_or_default();
            let hits = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine.scan_by_index(&name, &value_prefix).await
            })?;
            if json {
                print!("{{\"items\":[");
                for (i, (sec, pk)) in hits.iter().enumerate() {
                    if i > 0 {
                        print!(",");
                    }
                    print!(
                        "{{\"secondary\":{},\"primary\":{}}}",
                        json_string(&String::from_utf8_lossy(sec)),
                        json_string(&String::from_utf8_lossy(pk))
                    );
                }
                println!("]}}");
            } else {
                for (sec, pk) in hits {
                    println!(
                        "{} {}",
                        String::from_utf8_lossy(&sec),
                        String::from_utf8_lossy(&pk)
                    );
                }
            }
            Ok(())
        }
        "verify" => {
            let name = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument("usage: kayactl index verify <name>")
            })?;
            let div = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                engine.verify_index(&name).await
            })?;
            if json {
                print!("{{\"divergences\":[");
                for (i, d) in div.iter().enumerate() {
                    if i > 0 {
                        print!(",");
                    }
                    let kind = match &d.kind {
                        IndexDivergenceKind::MissingInIndex { expected_secondary } => format!(
                            "{{\"type\":\"missing\",\"expected_secondary\":{}}}",
                            json_string(&String::from_utf8_lossy(expected_secondary))
                        ),
                        IndexDivergenceKind::ExtraInIndex { secondary } => format!(
                            "{{\"type\":\"extra\",\"secondary\":{}}}",
                            json_string(&String::from_utf8_lossy(secondary))
                        ),
                    };
                    print!(
                        "{{\"primary\":{},\"kind\":{}}}",
                        json_string(&String::from_utf8_lossy(&d.primary_key)),
                        kind
                    );
                }
                println!("],\"ok\":{}}}", div.is_empty());
            } else if div.is_empty() {
                println!("OK index {name} is consistent (0 divergences)");
            } else {
                println!("FAIL index {name}: {} divergence(s)", div.len());
                for d in &div {
                    match &d.kind {
                        IndexDivergenceKind::MissingInIndex { expected_secondary } => {
                            println!(
                                "  missing primary={} expected_secondary={}",
                                String::from_utf8_lossy(&d.primary_key),
                                String::from_utf8_lossy(expected_secondary)
                            );
                        }
                        IndexDivergenceKind::ExtraInIndex { secondary } => {
                            println!(
                                "  extra primary={} secondary={}",
                                String::from_utf8_lossy(&d.primary_key),
                                String::from_utf8_lossy(secondary)
                            );
                        }
                    }
                }
                return Err(KayaError::corruption(format!(
                    "index {name} has {} divergence(s)",
                    div.len()
                )));
            }
            Ok(())
        }
        "backfill" => {
            let action = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument(
                    "usage: kayactl index backfill <pause|resume|step|status> <name> [--batch N]",
                )
            })?;
            let name = args.get(1).cloned().ok_or_else(|| {
                KayaError::invalid_argument(
                    "usage: kayactl index backfill <pause|resume|step|status> <name> [--batch N]",
                )
            })?;
            let mut rest = args[2..].to_vec();
            let batch: usize = remove_value_flag(&mut rest, "--batch")
                .map(|s| {
                    s.parse()
                        .map_err(|e| KayaError::invalid_argument(format!("--batch: {e}")))
                })
                .transpose()?
                .unwrap_or(64);

            let prog = block_on(async {
                let mut engine = open_engine(data_dir, durability).await?;
                match action.as_str() {
                    "pause" => engine.index_backfill_pause(&name),
                    "resume" => engine.index_backfill_resume(&name),
                    "status" => engine.index_backfill_status(&name),
                    "step" => engine.index_backfill_step(&name, batch).await,
                    other => Err(KayaError::invalid_argument(format!(
                        "unknown backfill action {other:?}"
                    ))),
                }
            })?;

            if json {
                println!(
                    "{{\"name\":{},\"status\":\"{}\",\"scanned\":{},\"indexed\":{}}}",
                    json_string(&name),
                    status_str(prog.status),
                    prog.scanned,
                    prog.indexed
                );
            } else {
                println!(
                    "index {name}: status={} scanned={} indexed={}",
                    status_str(prog.status),
                    prog.scanned,
                    prog.indexed
                );
            }
            Ok(())
        }
        _ => Err(KayaError::invalid_argument(index_usage())),
    }
}

fn index_usage() -> &'static str {
    "usage: kayactl index <create|list|drop|scan|verify|backfill> ..."
}
