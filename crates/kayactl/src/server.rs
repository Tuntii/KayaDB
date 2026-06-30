use std::net::SocketAddr;
use std::time::Duration;

use kaya_core::{KayaError, Result};
use kaya_net::{
    decode_error_payload, decode_scan_response, decode_value_payload, encode_admin_payload,
    encode_client_auth_payload, encode_key_payload, encode_member_payload, encode_put_payload,
    encode_remove_member_payload, encode_scan_payload, roundtrip, ADD_MEMBER_OPCODE,
    REMOVE_MEMBER_OPCODE, STATUS_ERROR, STATUS_INVALID_ARGUMENT, STATUS_NOT_FOUND,
    STATUS_NOT_LEADER, STATUS_OK,
};

use crate::cli::{block_on, json_string, print_usage};
use crate::stats_cmd;

fn with_client_auth(inner: &[u8], client_token: &Option<String>) -> Vec<u8> {
    encode_client_auth_payload(inner, client_token.as_deref())
}

pub(crate) fn run_server_mode(
    args: Vec<String>,
    endpoints: Vec<SocketAddr>,
    json: bool,
    timeout: Option<Duration>,
    operator_token: Option<String>,
    client_token: Option<String>,
) -> Result<()> {
    block_on(async move {
        run_server_mode_async(args, endpoints, json, timeout, operator_token, client_token).await
    })
}

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

async fn run_server_mode_async(
    args: Vec<String>,
    endpoints: Vec<SocketAddr>,
    json: bool,
    timeout: Option<Duration>,
    operator_token: Option<String>,
    client_token: Option<String>,
) -> Result<()> {
    // operator_token / client_token already parsed from flag/env at top level (global)

    match args.as_slice() {
        [] => {
            print_usage();
            Ok(())
        }
        [cmd] if cmd == "put" => Err(KayaError::invalid_argument(
            "usage: kayactl --server <addr> put <key> <value>",
        )),
        [cmd, key, value] if cmd == "put" => {
            let inner = encode_put_payload(key.as_bytes(), value.as_bytes());
            let payload = with_client_auth(&inner, &client_token);
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
            let inner = encode_key_payload(key.as_bytes());
            let payload = with_client_auth(&inner, &client_token);
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
            let inner = encode_key_payload(key.as_bytes());
            let payload = with_client_auth(&inner, &client_token);
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
            let inner = encode_scan_payload(prefix.as_bytes());
            let payload = with_client_auth(&inner, &client_token);
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
            let payload = with_client_auth(&[], &client_token);
            let (status, body) = roundtrip_with_retry(&endpoints, 6, &payload, timeout).await?;
            if status == STATUS_OK {
                let stats_str =
                    String::from_utf8(body).map_err(|e| KayaError::corruption(e.to_string()))?;
                if json {
                    println!("{}", stats_str);
                } else {
                    stats_cmd::print_human_stats_from_json(&stats_str);
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
            let (status, body) =
                roundtrip_with_retry(&endpoints, ADD_MEMBER_OPCODE, &payload, timeout).await?;
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
            let (status, body) =
                roundtrip_with_retry(&endpoints, REMOVE_MEMBER_OPCODE, &payload, timeout).await?;
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
