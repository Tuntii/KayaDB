mod backup;
mod cli;
#[cfg(feature = "ebpf")]
mod ebpf;
#[cfg(feature = "ebpf")]
mod ebpf_bpftrace;
#[cfg(feature = "ebpf")]
mod ebpf_correlate;
mod index_cmd;
mod inspect;
mod local;
mod range_cmd;
mod server;
mod stats_cmd;
mod watch;

use std::env;
use std::net::SocketAddr;
use std::process;
use std::time::Duration;

use kaya_core::{DurabilityMode, KayaError, Result};

use cli::{remove_all_value_flags, remove_flag, remove_value_flag};

fn main() {
    if let Err(error) = run() {
        eprintln!("ERROR: {error}");
        // Surface actionable recovery guidance for known-recoverable errors so
        // the operator is told what to do next, not just what failed.
        if let Some(hint) = error.guidance() {
            eprintln!("HINT: {hint}");
        }
        process::exit(error.exit_code());
    }
}

fn run() -> Result<()> {
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let json = remove_flag(&mut args, "--json");

    let operator_token = remove_value_flag(&mut args, "--operator-token")
        .or_else(|| env::var("KAYA_OPERATOR_TOKEN").ok())
        .filter(|t| !t.trim().is_empty());

    let client_token = remove_value_flag(&mut args, "--client-token")
        .or_else(|| env::var("KAYA_CLIENT_TOKEN").ok())
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

    let interval_secs: u64 = remove_value_flag(&mut args, "--interval")
        .map(|s| {
            s.parse::<u64>().map_err(|e| {
                KayaError::invalid_argument(format!("--interval: {e}; expected positive integer"))
            })
        })
        .transpose()?
        .unwrap_or(2);
    if interval_secs == 0 {
        return Err(KayaError::invalid_argument(
            "--interval must be at least 1 second",
        ));
    }
    let watch_interval = Duration::from_secs(interval_secs);

    if !args.is_empty() && args[0] == "ebpf" {
        #[cfg(not(feature = "ebpf"))]
        {
            println!(
                "eBPF CLI requires kayactl built with --features ebpf. See scripts/ebpf/ for bpftrace helpers."
            );
            return Ok(());
        }
        #[cfg(feature = "ebpf")]
        {
            let sub = if args.len() > 1 {
                args[1].clone()
            } else {
                "help".to_owned()
            };
            args.drain(0..2.min(args.len()));
            if sub == "trace" {
                let trace_sub = if args.is_empty() {
                    return Err(KayaError::invalid_argument(
                        "usage: kayactl ebpf trace wal [--data <dir>]",
                    ));
                } else {
                    args.remove(0)
                };
                return ebpf::handle_ebpf_trace(&trace_sub, &data_dir, json);
            }
            let run = remove_flag(&mut args, "--run");
            let duration_secs = if run {
                remove_value_flag(&mut args, "--duration")
                    .map(|s| s.trim_end_matches('s').parse::<u64>())
                    .transpose()
                    .map_err(|_| {
                        KayaError::invalid_argument(
                            "--duration must be a positive integer (seconds)",
                        )
                    })?
                    .unwrap_or(10)
            } else {
                10
            };
            let pid: Option<u32> =
                remove_value_flag(&mut args, "--pid").and_then(|s| s.parse().ok());
            return ebpf::handle_ebpf(&sub, &data_dir, pid, json, run, duration_secs, durability);
        }
    }

    if args.first().map(String::as_str) == Some("watch") {
        return watch::run_watch(
            args,
            watch::WatchContext {
                data_dir,
                durability,
                json,
                latency_view,
                server_addrs,
                timeout,
                interval: watch_interval,
                client_token,
            },
        );
    }

    // ── range (cluster meta table; requires --server) ─────────────────────────
    if args.first().map(String::as_str) == Some("range") {
        return range_cmd::run_range(args, server_addrs, timeout, client_token, json);
    }

    // ── server mode ───────────────────────────────────────────────────────────
    if !server_addrs.is_empty() {
        return server::run_server_mode(
            args,
            server_addrs,
            json,
            timeout,
            operator_token,
            client_token,
        );
    }

    // ── backup (filesystem copy of a node's data dir) ─────────────────────────
    if args.first().map(String::as_str) == Some("backup") {
        let mut backup_args = args;
        if json {
            backup_args.push("--json".to_owned());
        }
        return backup::run_backup(backup_args, &data_dir, durability);
    }

    // ── index (local secondary indexes) ───────────────────────────────────────
    if args.first().map(String::as_str) == Some("index") {
        return index_cmd::run_index(args, data_dir, durability, json);
    }

    // ── local engine mode ─────────────────────────────────────────────────────
    local::run_local_mode(args, data_dir, durability, json, latency_view)
}
