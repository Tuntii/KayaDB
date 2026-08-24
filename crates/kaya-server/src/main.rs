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
//!
//! ## Joining an existing cluster (node 4)
//!
//!   kayadb-server --join-cluster --node-id 4 --raft-addr 127.0.0.1:7484 \
//!       --client-addr 127.0.0.1:7383 \
//!       --peer 1=127.0.0.1:7481,127.0.0.1:7379 \
//!       --peer 2=127.0.0.1:7482,127.0.0.1:7380 \
//!       --peer 3=127.0.0.1:7483,127.0.0.1:7381 --data ./data4
//!
//! Then ask the leader to add the node (client opcode 7 / ADD_MEMBER).

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;

use kaya_server::security::{security_banner, validate_bind_addr};
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

    let allow_public_bind = args.iter().any(|a| a == "--allow-public-bind");
    if allow_public_bind {
        args.retain(|a| a != "--allow-public-bind");
    }

    let join_cluster = args.iter().any(|a| a == "--join-cluster");
    if join_cluster {
        args.retain(|a| a != "--join-cluster");
    }

    let audit_log_flag = if args.iter().any(|a| a == "--audit-log") {
        args.retain(|a| a != "--audit-log");
        Some(true)
    } else if args.iter().any(|a| a == "--no-audit-log") {
        args.retain(|a| a != "--no-audit-log");
        Some(false)
    } else {
        None
    };

    let ebpf_flag = args.iter().any(|a| a == "--ebpf");
    if ebpf_flag {
        args.retain(|a| a != "--ebpf");
    }
    #[cfg(feature = "ebpf")]
    let ebpf_enabled = ebpf_flag;
    #[cfg(not(feature = "ebpf"))]
    if ebpf_flag {
        return Err(
            "eBPF support not compiled in; rebuild kayadb-server with --features ebpf".to_owned(),
        );
    }

    let otel_flag = args.iter().any(|a| a == "--otel");
    if otel_flag {
        args.retain(|a| a != "--otel");
    }
    #[cfg(feature = "otel")]
    let otel_enabled = otel_flag;
    #[cfg(not(feature = "otel"))]
    if otel_flag {
        return Err(
            "OpenTelemetry support not compiled in; rebuild kayadb-server with --features otel"
                .to_owned(),
        );
    }

    #[cfg(feature = "ebpf")]
    let ebpf_seed: u64 = if ebpf_enabled {
        take_value(&mut args, "--ebpf-seed")
            .map(|s| s.parse::<u64>().map_err(|e| format!("--ebpf-seed: {e}")))
            .transpose()?
            .unwrap_or(42)
    } else {
        0
    };

    let no_metrics = args.iter().any(|a| a == "--no-metrics");
    if no_metrics {
        args.retain(|a| a != "--no-metrics");
    }

    // --drain / KAYA_DRAIN=1: decommission drain mode (status JSON reports "drain": true).
    let drain_flag = args.iter().any(|a| a == "--drain");
    if drain_flag {
        args.retain(|a| a != "--drain");
    }
    let drain_env = env::var("KAYA_DRAIN")
        .map(|v| {
            let t = v.trim();
            t == "1" || t.eq_ignore_ascii_case("true") || t.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false);
    let drain = drain_flag || drain_env;

    let metrics_addr: Option<SocketAddr> = if no_metrics {
        None
    } else {
        Some(
            take_value(&mut args, "--metrics-addr")
                .unwrap_or_else(|| "127.0.0.1:9090".to_owned())
                .parse()
                .map_err(|e| format!("--metrics-addr: {e}"))?,
        )
    };

    // Optional read-only JSON dashboard (M22). Example: --dashboard-addr 127.0.0.1:7380
    let dashboard_addr: Option<SocketAddr> = take_value(&mut args, "--dashboard-addr")
        .map(|s| s.parse().map_err(|e| format!("--dashboard-addr: {e}")))
        .transpose()?;

    // --audit-syslog <host:port> / KAYA_AUDIT_SYSLOG — forward audit records to
    // a remote SIEM collector over UDP (RFC 5424). Requires --audit-log.
    let audit_syslog: Option<SocketAddr> = match take_value(&mut args, "--audit-syslog")
        .or_else(|| std::env::var("KAYA_AUDIT_SYSLOG").ok())
        .filter(|v| !v.trim().is_empty())
    {
        Some(v) => Some(v.parse().map_err(|e| format!("--audit-syslog: {e}"))?),
        None => None,
    };

    validate_bind_addr(raft_addr, allow_public_bind)?;
    validate_bind_addr(client_addr, allow_public_bind)?;
    if let Some(addr) = metrics_addr {
        validate_bind_addr(addr, allow_public_bind)?;
    }
    if let Some(addr) = dashboard_addr {
        validate_bind_addr(addr, allow_public_bind)?;
    }
    eprintln!("{}", security_banner(allow_public_bind));

    // --peer <id>=<raft_addr>,<client_addr>  (may appear multiple times)
    let mut peers: Vec<(u64, SocketAddr, SocketAddr)> = Vec::new();
    loop {
        match take_value(&mut args, "--peer") {
            None => break,
            Some(spec) => {
                let peer = parse_peer(&spec)?;
                peers.push(peer);
            }
        }
    }

    if join_cluster && peers.is_empty() {
        return Err("--join-cluster requires at least one --peer seed address".to_owned());
    }

    let max_client_connections = take_value(&mut args, "--max-client-connections")
        .or_else(|| env::var("KAYA_MAX_CLIENT_CONNECTIONS").ok())
        .map(|s| {
            s.parse::<usize>()
                .map_err(|e| format!("--max-client-connections: {e}"))
        })
        .transpose()?;

    let operator_token =
        take_value(&mut args, "--operator-token").or_else(|| env::var("KAYA_OPERATOR_TOKEN").ok());

    let client_token =
        take_value(&mut args, "--client-token").or_else(|| env::var("KAYA_CLIENT_TOKEN").ok());

    // --acl-file <path> / KAYA_ACL_FILE — JSON map prefix -> token (M24 per-prefix ACL).
    let acl_file = take_value(&mut args, "--acl-file").or_else(|| env::var("KAYA_ACL_FILE").ok());

    // --encryption-key-file <path> / KAYA_ENCRYPTION_KEY_FILE — 32 raw bytes AES-256 key
    // (single, non-rotating key). Mutually exclusive with --encryption-keyring-file.
    let encryption_key_file = take_value(&mut args, "--encryption-key-file")
        .or_else(|| env::var("KAYA_ENCRYPTION_KEY_FILE").ok());

    // --encryption-keyring-file <path> / KAYA_ENCRYPTION_KEYRING_FILE — active + previous
    // keys for online rotation (#28). See `kayactl encryption` and docs/security.md §7.
    let encryption_keyring_file = take_value(&mut args, "--encryption-keyring-file")
        .or_else(|| env::var("KAYA_ENCRYPTION_KEYRING_FILE").ok());

    let tls_cert = take_value(&mut args, "--tls-cert").or_else(|| env::var("KAYA_TLS_CERT").ok());
    let tls_key = take_value(&mut args, "--tls-key").or_else(|| env::var("KAYA_TLS_KEY").ok());
    let tls_ca = take_value(&mut args, "--tls-ca").or_else(|| env::var("KAYA_TLS_CA").ok());

    let enable_tls = tls_cert.is_some() && tls_key.is_some();

    if !args.is_empty() {
        return Err(format!("unexpected arguments: {:?}", args));
    }

    let mut config = ClusterConfig::new(
        node_id,
        PathBuf::from(data_dir),
        raft_addr,
        client_addr,
        peers,
    );
    if let Some(tok) = operator_token {
        if !tok.trim().is_empty() {
            config = config.with_operator_token(tok);
        }
    }
    if let Some(tok) = client_token {
        if !tok.trim().is_empty() {
            config = config.with_client_token(tok);
        }
    }
    if let Some(path) = acl_file {
        let acl = kaya_server::acl::PrefixAcl::load_file(&path)
            .map_err(|e| format!("--acl-file {path}: {e}"))?;
        config = config.with_acl(acl);
    }
    match (encryption_key_file, encryption_keyring_file) {
        (Some(_), Some(_)) => {
            return Err(
                "--encryption-key-file and --encryption-keyring-file are mutually exclusive"
                    .to_owned(),
            );
        }
        (Some(path), None) => {
            let key = kaya_io::load_key_file(&path)
                .map_err(|e| format!("--encryption-key-file {path}: {e}"))?;
            config = config.with_encryption_key(key);
        }
        (None, Some(path)) => {
            let keyring = kaya_io::load_keyring_file(&path)
                .map_err(|e| format!("--encryption-keyring-file {path}: {e}"))?;
            config = config.with_encryption_keyring(keyring);
        }
        (None, None) => {}
    }

    let audit_log = audit_log_flag.unwrap_or_else(|| {
        config.operator_token.is_some() || config.client_token.is_some() || config.acl.is_some()
    });
    config = config.with_audit_log(audit_log);
    config = config.with_audit_syslog(audit_syslog);

    config = match metrics_addr {
        Some(addr) => config.with_metrics_addr(addr),
        None => config.without_metrics(),
    };
    if let Some(addr) = dashboard_addr {
        config = config.with_dashboard_addr(addr);
    }

    if enable_tls {
        if let (Some(cert), Some(key)) = (tls_cert, tls_key) {
            let tls = kaya_net::TlsConfig {
                cert_path: cert.into(),
                key_path: key.into(),
                ca_path: tls_ca.map(Into::into),
                require_client_cert: true, // mTLS for peers
            };
            config = config.with_tls(tls);
        }
    }
    if join_cluster {
        config = config.with_join_cluster();
    }
    if drain {
        config = config.with_drain();
    }
    if let Some(max) = max_client_connections {
        config = config.with_max_client_connections(max);
    }

    #[cfg(feature = "ebpf")]
    if ebpf_enabled {
        config = config.with_ebpf(ebpf_seed);
    }

    #[cfg(feature = "otel")]
    if otel_enabled {
        config = config.with_otel();
    }

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

/// Parse a `<id>=<raft_addr>,<client_addr>` or `<id>=<raft_addr>` peer specification.
fn parse_peer(spec: &str) -> Result<(u64, SocketAddr, SocketAddr), String> {
    let (id_str, addrs_str) = spec
        .split_once('=')
        .ok_or_else(|| format!("peer spec must be <id>=<addr>, got: {spec}"))?;
    let id = id_str
        .parse::<u64>()
        .map_err(|e| format!("peer id '{id_str}': {e}"))?;

    let (raft_addr, client_addr) = if let Some((raft_str, client_str)) = addrs_str.split_once(',') {
        let r_addr = raft_str
            .parse::<SocketAddr>()
            .map_err(|e| format!("peer raft addr '{raft_str}': {e}"))?;
        let c_addr = client_str
            .parse::<SocketAddr>()
            .map_err(|e| format!("peer client addr '{client_str}': {e}"))?;
        (r_addr, c_addr)
    } else {
        let r_addr = addrs_str
            .parse::<SocketAddr>()
            .map_err(|e| format!("peer addr '{addrs_str}': {e}"))?;
        (r_addr, r_addr)
    };
    Ok((id, raft_addr, client_addr))
}
