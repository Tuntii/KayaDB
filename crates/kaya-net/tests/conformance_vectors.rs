//! Protocol conformance vectors loaded from `docs/clients/conformance/vectors.json`.
//!
//! Run: `cargo test -p kaya-net conformance_vectors`

use kaya_net::{
    decode_admin_payload, decode_client_auth_payload, decode_error_payload, decode_key_payload,
    decode_put_payload, decode_scan_response, encode_admin_payload, encode_client_auth_payload,
    encode_error_payload, encode_key_payload, encode_put_payload, encode_scan_response,
};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
struct ConformanceVector {
    name: String,
    #[serde(rename = "fn")]
    function: String,
    input: VectorInput,
    expect_ok: bool,
}

#[derive(Debug, Default, Deserialize)]
struct VectorInput {
    key: Option<String>,
    value: Option<String>,
    key_hex: Option<String>,
    value_hex: Option<String>,
    message: Option<String>,
    opcode: Option<u8>,
    inner: Option<String>,
    inner_hex: Option<String>,
    token: Option<String>,
    items: Option<Vec<ScanItemInput>>,
    raw_hex: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScanItemInput {
    key: Option<String>,
    value: Option<String>,
    key_hex: Option<String>,
    value_hex: Option<String>,
}

type ScanItems = Vec<(Vec<u8>, Vec<u8>)>;

fn vectors_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/clients/conformance/vectors.json")
}

fn load_vectors() -> Vec<ConformanceVector> {
    let path = vectors_path();
    let raw = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    serde_json::from_str(&raw).unwrap_or_else(|e| panic!("failed to parse {}: {e}", path.display()))
}

fn decode_hex(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err(format!("odd hex length: {}", s.len()));
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

fn bytes_from_fields(
    text: Option<&str>,
    hex: Option<&str>,
    field: &str,
) -> Result<Vec<u8>, String> {
    match (text, hex) {
        (_, Some(h)) => decode_hex(h).map_err(|e| format!("{field}_hex: {e}")),
        (Some(s), None) => Ok(s.as_bytes().to_vec()),
        (None, None) => Ok(Vec::new()),
    }
}

fn scan_items_from_input(items: &[ScanItemInput]) -> Result<ScanItems, String> {
    items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let key = bytes_from_fields(item.key.as_deref(), item.key_hex.as_deref(), "key")
                .map_err(|e| format!("items[{i}].{e}"))?;
            let value =
                bytes_from_fields(item.value.as_deref(), item.value_hex.as_deref(), "value")
                    .map_err(|e| format!("items[{i}].{e}"))?;
            Ok((key, value))
        })
        .collect()
}

fn run_vector(vector: &ConformanceVector) {
    let result = match vector.function.as_str() {
        "put_roundtrip" => run_put_roundtrip(&vector.input),
        "key_roundtrip" => run_key_roundtrip(&vector.input),
        "admin_roundtrip" => run_admin_roundtrip(&vector.input),
        "client_roundtrip" => run_client_roundtrip(&vector.input),
        "scan_roundtrip" => run_scan_roundtrip(&vector.input),
        "error_roundtrip" => run_error_roundtrip(&vector.input),
        other => panic!("unknown fn '{other}' in vector '{}'", vector.name),
    };

    match (result, vector.expect_ok) {
        (Ok(()), true) => {}
        (Err(_), false) => {}
        (Ok(()), false) => panic!(
            "vector '{}' ({}) expected failure but succeeded",
            vector.name, vector.function
        ),
        (Err(e), true) => panic!(
            "vector '{}' ({}) expected success but failed: {e}",
            vector.name, vector.function
        ),
    }
}

fn run_put_roundtrip(input: &VectorInput) -> Result<(), String> {
    if let Some(raw) = &input.raw_hex {
        return decode_put_payload(&decode_hex(raw)?).map(|_| ());
    }
    let key = bytes_from_fields(input.key.as_deref(), input.key_hex.as_deref(), "key")?;
    let value = bytes_from_fields(input.value.as_deref(), input.value_hex.as_deref(), "value")?;
    let encoded = encode_put_payload(&key, &value);
    let (decoded_key, decoded_value) = decode_put_payload(&encoded)?;
    if decoded_key != key || decoded_value != value {
        return Err(format!(
            "put mismatch: got ({decoded_key:?}, {decoded_value:?}), want ({key:?}, {value:?})"
        ));
    }
    Ok(())
}

fn run_key_roundtrip(input: &VectorInput) -> Result<(), String> {
    if let Some(raw) = &input.raw_hex {
        return decode_key_payload(&decode_hex(raw)?).map(|_| ());
    }
    let key = bytes_from_fields(input.key.as_deref(), input.key_hex.as_deref(), "key")?;
    let encoded = encode_key_payload(&key);
    let decoded = decode_key_payload(&encoded)?;
    if decoded != key {
        return Err(format!("key mismatch: got {decoded:?}, want {key:?}"));
    }
    Ok(())
}

fn run_admin_roundtrip(input: &VectorInput) -> Result<(), String> {
    let opcode = input
        .opcode
        .ok_or_else(|| "admin_roundtrip requires opcode".to_owned())?;
    let inner = bytes_from_fields(input.inner.as_deref(), input.inner_hex.as_deref(), "inner")?;
    let token = input.token.as_deref();
    let encoded = encode_admin_payload(opcode, &inner, token);
    let (decoded_opcode, decoded_inner, decoded_token) = decode_admin_payload(&encoded)?;
    if decoded_opcode != opcode {
        return Err(format!(
            "opcode mismatch: got {decoded_opcode}, want {opcode}"
        ));
    }
    if decoded_inner != inner {
        return Err(format!(
            "inner mismatch: got {decoded_inner:?}, want {inner:?}"
        ));
    }
    if decoded_token.as_deref() != token {
        return Err(format!(
            "token mismatch: got {:?}, want {:?}",
            decoded_token.as_deref(),
            token
        ));
    }
    Ok(())
}

fn run_client_roundtrip(input: &VectorInput) -> Result<(), String> {
    let inner = bytes_from_fields(input.inner.as_deref(), input.inner_hex.as_deref(), "inner")?;
    let token = input.token.as_deref();
    let encoded = encode_client_auth_payload(&inner, token);
    let (decoded_inner, decoded_token) = decode_client_auth_payload(&encoded)?;
    if decoded_inner != inner {
        return Err(format!(
            "inner mismatch: got {decoded_inner:?}, want {inner:?}"
        ));
    }
    if decoded_token.as_deref() != token {
        return Err(format!(
            "token mismatch: got {:?}, want {:?}",
            decoded_token.as_deref(),
            token
        ));
    }
    Ok(())
}

fn run_scan_roundtrip(input: &VectorInput) -> Result<(), String> {
    if let Some(raw) = &input.raw_hex {
        return decode_scan_response(&decode_hex(raw)?).map(|_| ());
    }
    let items = scan_items_from_input(input.items.as_deref().unwrap_or(&[]))?;
    let encoded = encode_scan_response(&items);
    let decoded = decode_scan_response(&encoded)?;
    if decoded != items {
        return Err(format!("scan mismatch: got {decoded:?}, want {items:?}"));
    }
    Ok(())
}

fn run_error_roundtrip(input: &VectorInput) -> Result<(), String> {
    if let Some(raw) = &input.raw_hex {
        return decode_error_payload(&decode_hex(raw)?).map(|_| ());
    }
    let message = input
        .message
        .as_deref()
        .ok_or_else(|| "error_roundtrip requires message".to_owned())?;
    let encoded = encode_error_payload(message);
    let decoded = decode_error_payload(&encoded)?;
    if decoded != message {
        return Err(format!(
            "message mismatch: got {decoded:?}, want {message:?}"
        ));
    }
    Ok(())
}

#[test]
fn conformance_vectors_load_file() {
    let vectors = load_vectors();
    assert!(
        vectors.len() >= 20,
        "expected at least 20 conformance vectors, got {}",
        vectors.len()
    );
}

#[test]
fn conformance_vectors_run_all() {
    let vectors = load_vectors();
    for vector in &vectors {
        run_vector(vector);
    }
}
