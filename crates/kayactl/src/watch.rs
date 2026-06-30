use std::io::{self, IsTerminal, Write};
use std::net::SocketAddr;
use std::time::Duration;

use kaya_core::{DurabilityMode, KayaError, Result};
use kaya_net::{
    decode_error_payload, encode_client_auth_payload, roundtrip, STATUS_ERROR,
    STATUS_INVALID_ARGUMENT, STATUS_NOT_LEADER, STATUS_OK,
};

use crate::cli::block_on;
use crate::stats_cmd;

fn clear_screen() {
    if io::stdout().is_terminal() {
        let _ = write!(io::stdout(), "\x1b[2J\x1b[H");
        let _ = io::stdout().flush();
    }
}

fn print_timestamp_header() {
    if !io::stdout().is_terminal() {
        let now = chrono_lite_now();
        println!("--- {now} ---");
    }
}

/// Minimal UTC timestamp without pulling in chrono.
fn chrono_lite_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!("unix={secs}")
}

pub(crate) struct WatchContext {
    pub data_dir: String,
    pub durability: DurabilityMode,
    pub json: bool,
    pub latency_view: bool,
    pub server_addrs: Vec<SocketAddr>,
    pub timeout: Option<Duration>,
    pub interval: Duration,
    pub client_token: Option<String>,
}

pub(crate) fn run_watch(args: Vec<String>, ctx: WatchContext) -> Result<()> {
    match args.as_slice() {
        [cmd, sub] if cmd == "watch" && sub == "status" => {
            if !ctx.server_addrs.is_empty() {
                run_watch_server(
                    ctx.server_addrs,
                    ctx.json,
                    ctx.timeout,
                    ctx.interval,
                    ctx.client_token,
                )
            } else {
                run_watch_local(
                    ctx.data_dir,
                    ctx.durability,
                    ctx.json,
                    ctx.latency_view,
                    ctx.interval,
                )
            }
        }
        [cmd] if cmd == "watch" => Err(KayaError::invalid_argument(
            "usage: kayactl watch [--interval <secs>] [--server <addr>] status",
        )),
        [cmd, other] if cmd == "watch" => Err(KayaError::invalid_argument(format!(
            "unknown watch subcommand: {other}; supported: status"
        ))),
        _ => Err(KayaError::invalid_argument(
            "usage: kayactl watch [--interval <secs>] [--server <addr>] status",
        )),
    }
}

fn run_watch_local(
    data_dir: String,
    durability: DurabilityMode,
    json: bool,
    latency_view: bool,
    interval: Duration,
) -> Result<()> {
    loop {
        clear_screen();
        print_timestamp_header();
        stats_cmd::run_local_stats(data_dir.clone(), durability, json, latency_view)?;
        io::stdout().flush().ok();
        std::thread::sleep(interval);
    }
}

fn run_watch_server(
    endpoints: Vec<SocketAddr>,
    json: bool,
    timeout: Option<Duration>,
    interval: Duration,
    client_token: Option<String>,
) -> Result<()> {
    block_on(async move {
        loop {
            clear_screen();
            print_timestamp_header();
            fetch_and_print_server_status(&endpoints, json, timeout, &client_token).await?;
            io::stdout().flush().ok();
            tokio::time::sleep(interval).await;
        }
    })
}

async fn fetch_and_print_server_status(
    endpoints: &[SocketAddr],
    json: bool,
    timeout: Option<Duration>,
    client_token: &Option<String>,
) -> Result<()> {
    let inner: &[u8] = &[];
    let payload = encode_client_auth_payload(inner, client_token.as_deref());
    let mut addr_idx = 0usize;
    let mut redirect_retries = 0u32;
    let mut current_addr = endpoints[0];

    loop {
        let request = roundtrip(current_addr, 6, &payload);
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

        if status == STATUS_NOT_LEADER && redirect_retries < 6 {
            tokio::time::sleep(Duration::from_millis(80)).await;
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

        if status == STATUS_OK {
            let stats_str =
                String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
            if json {
                println!("{}", stats_str);
            } else {
                stats_cmd::print_human_stats_from_json(&stats_str);
            }
            return Ok(());
        }

        return Err(KayaError::internal("status check failed"));
    }
}
