mod cli;
mod ebpf;
mod inspect;
mod local;
mod server;
mod stats_cmd;

use std::env;
use std::net::SocketAddr;
use std::process;
use std::time::Duration;

use kaya_core::{DurabilityMode, KayaError, Result};

use cli::{
    remove_all_value_flags, remove_flag, remove_value_flag,
};

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
        return ebpf::handle_ebpf(&sub, pid, run, duration, json);
    }

    // ── server mode ───────────────────────────────────────────────────────────
    if !server_addrs.is_empty() {
        return server::run_server_mode(
            args,
            server_addrs,
            json,
            timeout,
            latency_view,
            operator_token,
        );
    }

    // ── local engine mode ─────────────────────────────────────────────────────
    local::run_local_mode(args, data_dir, durability, json, latency_view)
}