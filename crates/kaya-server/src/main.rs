//! `kayadb-server` — KayaDB cluster node process.
//!
//! ## Single-node (stand-alone) mode
//!
//!   kayadb-server
//!
//! ## Three-node cluster example
//!
//!   # Node 1
//!   kayadb-server --node-id 1 --raft-addr 127.0.0.1:7481 --client-addr 127.0.0.1:7379 \
//!       --peer 2=127.0.0.1:7482 --peer 3=127.0.0.1:7483 --data ./data1
//!
//!   # Node 2
//!   kayadb-server --node-id 2 --raft-addr 127.0.0.1:7482 --client-addr 127.0.0.1:7380 \
//!       --peer 1=127.0.0.1:7481 --peer 3=127.0.0.1:7483 --data ./data2
//!
//!   # Node 3
//!   kayadb-server --node-id 3 --raft-addr 127.0.0.1:7483 --client-addr 127.0.0.1:7381 \
//!       --peer 1=127.0.0.1:7481 --peer 2=127.0.0.1:7482 --data ./data3

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;

use kaya_server::ClusterConfig;
use kaya_server::ClusterNode;

fn main() {
    if let Err(e) = run() {
        eprintln!("ERROR: {e}");
        process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args: Vec<String> = env::args().skip(1).collect();

    let node_id = take_value(&mut args, "--node-id")
        .map(|s| s.parse::<u64>().map_err(|e| format!("--node-id: {e}")))
        .transpose()?
        .unwrap_or(1);

    let raft_addr: SocketAddr = take_value(&mut args, "--raft-addr")
        .unwrap_or_else(|| "127.0.0.1:7481".to_owned())
        .parse()
        .map_err(|e| format!("--raft-addr: {e}"))?;

    let client_addr: SocketAddr = take_value(&mut args, "--client-addr")
        .unwrap_or_else(|| "127.0.0.1:7379".to_owned())
        .parse()
        .map_err(|e| format!("--client-addr: {e}"))?;

    let data_dir = take_value(&mut args, "--data").unwrap_or_else(|| "./data".to_owned());

    // --peer <id>=<addr>  (may appear multiple times)
    let mut peers: Vec<(u64, SocketAddr)> = Vec::new();
    loop {
        match take_value(&mut args, "--peer") {
            None => break,
            Some(spec) => {
                let peer = parse_peer(&spec)?;
                peers.push(peer);
            }
        }
    }

    if !args.is_empty() {
        return Err(format!("unexpected arguments: {:?}", args));
    }

    let config = ClusterConfig::new(
        node_id,
        PathBuf::from(data_dir),
        raft_addr,
        client_addr,
        peers,
    );

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(async move { ClusterNode::new(config).run().await })
        .map_err(|e| e.to_string())
}

/// Remove and return the value following `flag` from `args`.
fn take_value(args: &mut Vec<String>, flag: &str) -> Option<String> {
    let pos = args.iter().position(|a| a == flag)?;
    if pos + 1 < args.len() {
        args.remove(pos);
        Some(args.remove(pos))
    } else {
        None
    }
}

/// Parse a `<id>=<addr>` peer specification.
fn parse_peer(spec: &str) -> Result<(u64, SocketAddr), String> {
    let (id_str, addr_str) = spec
        .split_once('=')
        .ok_or_else(|| format!("peer spec must be <id>=<addr>, got: {spec}"))?;
    let id = id_str
        .parse::<u64>()
        .map_err(|e| format!("peer id '{id_str}': {e}"))?;
    let addr = addr_str
        .parse::<SocketAddr>()
        .map_err(|e| format!("peer addr '{addr_str}': {e}"))?;
    Ok((id, addr))
}
