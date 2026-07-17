//! `kayactl range` — list / split / merge meta ranges (M21/M22) via cluster server.

use std::net::SocketAddr;
use std::time::Duration;

use kaya_core::{KayaError, Result};
use kaya_net::{
    decode_error_payload, decode_list_ranges_response, encode_client_auth_payload,
    encode_merge_range_request, encode_split_range_request, roundtrip, LIST_RANGES_OPCODE,
    MERGE_RANGE_OPCODE, SPLIT_RANGE_OPCODE, STATUS_INVALID_ARGUMENT, STATUS_NOT_LEADER, STATUS_OK,
};

use crate::cli::{block_on, json_string};

/// Entry: args after global flags; may start with `range`.
pub fn run_range(
    mut args: Vec<String>,
    server_addrs: Vec<SocketAddr>,
    timeout: Option<Duration>,
    client_token: Option<String>,
    json: bool,
) -> Result<()> {
    if args.first().map(String::as_str) == Some("range") {
        args.remove(0);
    }
    if server_addrs.is_empty() {
        return Err(KayaError::invalid_argument(
            "kayactl range requires --server <addr> (cluster meta table)",
        ));
    }
    let sub = args
        .first()
        .cloned()
        .ok_or_else(|| KayaError::invalid_argument(range_usage()))?;
    args.remove(0);

    match sub.as_str() {
        "list" => block_on(async {
            let (status, body) =
                request(&server_addrs, LIST_RANGES_OPCODE, &[], timeout, &client_token).await?;
            if status != STATUS_OK {
                return status_err(status, &body);
            }
            let (meta_epoch, ranges) =
                decode_list_ranges_response(&body).map_err(KayaError::corruption)?;
            if json {
                print!("{{\"meta_epoch\":{meta_epoch},\"ranges\":[");
                for (i, (range_id, epoch, group_id, start, end)) in ranges.iter().enumerate() {
                    if i > 0 {
                        print!(",");
                    }
                    print!(
                        "{{\"range_id\":{range_id},\"epoch\":{epoch},\"group_id\":{group_id},\"start\":{},\"end\":{}}}",
                        json_string(&String::from_utf8_lossy(start)),
                        json_string(&String::from_utf8_lossy(end)),
                    );
                }
                println!("]}}");
            } else {
                println!("meta_epoch={meta_epoch}");
                for (range_id, epoch, group_id, start, end) in &ranges {
                    println!(
                        "range_id={range_id} epoch={epoch} group={group_id} start={:?} end={:?}",
                        String::from_utf8_lossy(start),
                        String::from_utf8_lossy(end),
                    );
                }
            }
            Ok(())
        }),
        "split" => {
            let key = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument("usage: kayactl --server <addr> range split <key>")
            })?;
            let payload = encode_split_range_request(key.as_bytes());
            block_on(async {
                let (status, body) = request(
                    &server_addrs,
                    SPLIT_RANGE_OPCODE,
                    &payload,
                    timeout,
                    &client_token,
                )
                .await?;
                if status != STATUS_OK {
                    return status_err(status, &body);
                }
                let (meta_epoch, halves) =
                    decode_list_ranges_response(&body).map_err(KayaError::corruption)?;
                if json {
                    println!(
                        "{{\"ok\":true,\"meta_epoch\":{meta_epoch},\"halves\":{}}}",
                        halves.len()
                    );
                } else {
                    println!(
                        "OK split at {key:?}; meta_epoch={meta_epoch} halves={}",
                        halves.len()
                    );
                    for (range_id, epoch, group_id, start, end) in &halves {
                        println!(
                            "  range_id={range_id} epoch={epoch} group={group_id} [{:?}, {:?})",
                            String::from_utf8_lossy(start),
                            String::from_utf8_lossy(end),
                        );
                    }
                }
                Ok(())
            })
        }
        "merge" => {
            let raw = args.first().cloned().ok_or_else(|| {
                KayaError::invalid_argument(
                    "usage: kayactl --server <addr> range merge <left-start-hex-or-utf8>",
                )
            })?;
            let left_start = parse_range_key(&raw)?;
            let payload = encode_merge_range_request(&left_start);
            block_on(async {
                let (status, body) = request(
                    &server_addrs,
                    MERGE_RANGE_OPCODE,
                    &payload,
                    timeout,
                    &client_token,
                )
                .await?;
                if status != STATUS_OK {
                    return status_err(status, &body);
                }
                let (meta_epoch, ranges) =
                    decode_list_ranges_response(&body).map_err(KayaError::corruption)?;
                if json {
                    println!(
                        "{{\"ok\":true,\"meta_epoch\":{meta_epoch},\"merged\":{}}}",
                        ranges.len()
                    );
                } else {
                    println!(
                        "OK merge left_start={raw:?}; meta_epoch={meta_epoch} merged={}",
                        ranges.len()
                    );
                    for (range_id, epoch, group_id, start, end) in &ranges {
                        println!(
                            "  range_id={range_id} epoch={epoch} group={group_id} [{:?}, {:?})",
                            String::from_utf8_lossy(start),
                            String::from_utf8_lossy(end),
                        );
                    }
                }
                Ok(())
            })
        }
        _ => Err(KayaError::invalid_argument(range_usage())),
    }
}

/// Parse a range key from CLI: empty / `@empty` / `""` → empty bytes;
/// `0x…` or `hex:…` → hex-decoded; otherwise UTF-8 bytes.
fn parse_range_key(raw: &str) -> Result<Vec<u8>> {
    if raw.is_empty() || raw == "@empty" || raw == "\"\"" {
        return Ok(Vec::new());
    }
    let hex_body = if let Some(rest) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        Some(rest)
    } else {
        raw.strip_prefix("hex:").or_else(|| raw.strip_prefix("HEX:"))
    };
    if let Some(hex) = hex_body {
        if hex.is_empty() {
            return Ok(Vec::new());
        }
        if hex.len() % 2 != 0 {
            return Err(KayaError::invalid_argument(
                "hex left-start must have even length",
            ));
        }
        let mut out = Vec::with_capacity(hex.len() / 2);
        let bytes = hex.as_bytes();
        for i in (0..bytes.len()).step_by(2) {
            let h = std::str::from_utf8(&bytes[i..i + 2]).unwrap_or("");
            let b = u8::from_str_radix(h, 16).map_err(|_| {
                KayaError::invalid_argument(format!("invalid hex digit in left-start: {h}"))
            })?;
            out.push(b);
        }
        return Ok(out);
    }
    Ok(raw.as_bytes().to_vec())
}

async fn request(
    endpoints: &[SocketAddr],
    opcode: u8,
    payload: &[u8],
    timeout: Option<Duration>,
    client_token: &Option<String>,
) -> Result<(u16, Vec<u8>)> {
    let wire = encode_client_auth_payload(payload, client_token.as_deref());
    let mut current = endpoints[0];
    let mut redirects = 0u32;
    loop {
        let fut = roundtrip(current, opcode, &wire);
        let (status, body) = match timeout {
            Some(dur) => match tokio::time::timeout(dur, fut).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => return Err(KayaError::internal(e.to_string())),
                Err(_) => {
                    return Err(KayaError::internal(format!(
                        "request to {current} timed out"
                    )));
                }
            },
            None => fut
                .await
                .map_err(|e| KayaError::internal(e.to_string()))?,
        };
        if status == STATUS_NOT_LEADER && redirects < 6 {
            if !body.is_empty() {
                if let Ok(s) = String::from_utf8(body.clone()) {
                    if let Ok(addr) = s.parse::<SocketAddr>() {
                        current = addr;
                    }
                }
            }
            redirects += 1;
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        return Ok((status, body));
    }
}

fn status_err(status: u16, body: &[u8]) -> Result<()> {
    if status == STATUS_INVALID_ARGUMENT {
        let msg = decode_error_payload(body).unwrap_or_else(|_| "invalid argument".into());
        return Err(KayaError::invalid_argument(msg));
    }
    let msg = decode_error_payload(body).unwrap_or_else(|_| format!("status {status}"));
    Err(KayaError::internal(msg))
}

fn range_usage() -> &'static str {
    "usage: kayactl --server <addr> range <list|split|merge> ..."
}
