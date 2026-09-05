//! Strict JSONL history format for the `kaya-wgl` explorer.
//!
//! One JSON object per operation, one object per line. Blank lines are ignored.
//! Unknown fields, missing required fields, and malformed values are errors.
//!
//! # Schema
//!
//! Common optional fields:
//! - `client` (u32): client id
//! - `start` / `end` (u64): half-open tick interval; `start < end` is required
//!   when either is present. If both are omitted, ticks are assigned sequentially.
//!
//! `op` is required and is one of `put`, `get`, `delete`, `scan` (lowercase).
//!
//! | op | required | `result` |
//! |----|----------|----------|
//! | `put` | `key`, `value` | `"ok"` or `{"error":"..."}` |
//! | `get` | `key` | string value, `null` (miss), or `{"error":"..."}` |
//! | `delete` | `key` | `"ok"` or `{"error":"..."}` |
//! | `scan` | `prefix` | `[[key, value], ...]` or `{"error":"..."}` |
//!
//! Byte fields (`key`, `value`, `prefix`, scan pairs) are UTF-8 strings, or hex
//! if they start with `0x` (even-length hex digits after the prefix).
//!
//! # Examples
//!
//! ```json
//! {"client":0,"start":1,"end":2,"op":"put","key":"k","value":"v","result":"ok"}
//! {"client":1,"start":1,"end":3,"op":"get","key":"k","result":"v"}
//! {"op":"get","key":"k","result":null}
//! {"op":"delete","key":"k","result":"ok"}
//! {"op":"scan","prefix":"a","result":[["a1","v"]]}
//! {"op":"put","key":"0x6b","value":"0x76","result":"ok"}
//! ```

use crate::linear::{HistoryEntry, LinearizabilityChecker, MinimalCounterexample, Op, OpResult};
use serde_json::{Map, Value};

/// Parse a JSONL history into a [`LinearizabilityChecker`].
pub fn parse_history_jsonl(input: &str) -> Result<LinearizabilityChecker, String> {
    let mut checker = LinearizabilityChecker::new();
    let mut auto_tick = 0u64;
    for (idx, raw) in input.lines().enumerate() {
        let line_no = idx + 1;
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let entry = parse_line(line, line_no, &mut auto_tick)?;
        checker.record_interval(
            entry.start_tick,
            entry.end_tick,
            entry.client_id,
            entry.op,
            entry.result,
        );
    }
    Ok(checker)
}

fn parse_line(line: &str, line_no: usize, auto_tick: &mut u64) -> Result<HistoryEntry, String> {
    let value: Value =
        serde_json::from_str(line).map_err(|e| format!("line {line_no}: invalid JSON ({e})"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| format!("line {line_no}: expected a JSON object"))?;

    let op_str = require_str(obj, "op", line_no)?;
    match op_str {
        "put" => {
            reject_unknown(
                obj,
                &["client", "start", "end", "op", "key", "value", "result"],
                line_no,
            )?;
            let key = parse_bytes_field(obj, "key", line_no)?;
            let value = parse_bytes_field(obj, "value", line_no)?;
            let result = parse_ok_or_error(obj, line_no)?;
            finish_entry(obj, line_no, auto_tick, Op::Put { key, value }, result)
        }
        "get" => {
            reject_unknown(
                obj,
                &["client", "start", "end", "op", "key", "result"],
                line_no,
            )?;
            let key = parse_bytes_field(obj, "key", line_no)?;
            let result = parse_get_result(obj, line_no)?;
            finish_entry(obj, line_no, auto_tick, Op::Get { key }, result)
        }
        "delete" => {
            reject_unknown(
                obj,
                &["client", "start", "end", "op", "key", "result"],
                line_no,
            )?;
            let key = parse_bytes_field(obj, "key", line_no)?;
            let result = parse_ok_or_error(obj, line_no)?;
            finish_entry(obj, line_no, auto_tick, Op::Delete { key }, result)
        }
        "scan" => {
            reject_unknown(
                obj,
                &["client", "start", "end", "op", "prefix", "result"],
                line_no,
            )?;
            let prefix = parse_bytes_field(obj, "prefix", line_no)?;
            let result = parse_scan_result(obj, line_no)?;
            finish_entry(obj, line_no, auto_tick, Op::Scan { prefix }, result)
        }
        other => Err(format!(
            "line {line_no}: unknown op {other:?} (expected put, get, delete, scan)"
        )),
    }
}

fn finish_entry(
    obj: &Map<String, Value>,
    line_no: usize,
    auto_tick: &mut u64,
    op: Op,
    result: OpResult,
) -> Result<HistoryEntry, String> {
    let client_id = optional_u32(obj, "client", line_no)?;
    let (start_tick, end_tick) = parse_interval(obj, line_no, auto_tick)?;
    Ok(HistoryEntry {
        start_tick,
        end_tick,
        client_id,
        op,
        result,
    })
}

fn parse_interval(
    obj: &Map<String, Value>,
    line_no: usize,
    auto_tick: &mut u64,
) -> Result<(u64, u64), String> {
    let start = optional_u64(obj, "start", line_no)?;
    let end = optional_u64(obj, "end", line_no)?;
    match (start, end) {
        (None, None) => {
            let s = *auto_tick;
            let e = s + 1;
            *auto_tick = e + 1;
            Ok((s, e))
        }
        (Some(_), None) | (None, Some(_)) => Err(format!(
            "line {line_no}: start and end must both be present or both omitted"
        )),
        (Some(s), Some(e)) => {
            if s >= e {
                return Err(format!("line {line_no}: start ({s}) must be < end ({e})"));
            }
            if e + 1 > *auto_tick {
                *auto_tick = e + 1;
            }
            Ok((s, e))
        }
    }
}

fn reject_unknown(
    obj: &Map<String, Value>,
    allowed: &[&str],
    line_no: usize,
) -> Result<(), String> {
    for key in obj.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(format!("line {line_no}: unknown field {key:?}"));
        }
    }
    Ok(())
}

fn require_field<'a>(
    obj: &'a Map<String, Value>,
    field: &str,
    line_no: usize,
) -> Result<&'a Value, String> {
    obj.get(field)
        .ok_or_else(|| format!("line {line_no}: missing field {field:?}"))
}

fn require_str<'a>(
    obj: &'a Map<String, Value>,
    field: &str,
    line_no: usize,
) -> Result<&'a str, String> {
    require_field(obj, field, line_no)?
        .as_str()
        .ok_or_else(|| format!("line {line_no}: field {field:?} must be a string"))
}

fn optional_u64(
    obj: &Map<String, Value>,
    field: &str,
    line_no: usize,
) -> Result<Option<u64>, String> {
    match obj.get(field) {
        None => Ok(None),
        Some(Value::Number(n)) => n.as_u64().map(Some).ok_or_else(|| {
            format!("line {line_no}: field {field:?} must be a non-negative integer")
        }),
        Some(_) => Err(format!(
            "line {line_no}: field {field:?} must be a non-negative integer"
        )),
    }
}

fn optional_u32(
    obj: &Map<String, Value>,
    field: &str,
    line_no: usize,
) -> Result<Option<u32>, String> {
    match optional_u64(obj, field, line_no)? {
        None => Ok(None),
        Some(n) => u32::try_from(n)
            .map(Some)
            .map_err(|_| format!("line {line_no}: field {field:?} exceeds u32")),
    }
}

fn parse_bytes_field(
    obj: &Map<String, Value>,
    field: &str,
    line_no: usize,
) -> Result<Vec<u8>, String> {
    let s = require_str(obj, field, line_no)?;
    parse_bytes(s).map_err(|e| format!("line {line_no}: field {field:?}: {e}"))
}

/// UTF-8 bytes, or hex if the string starts with `0x` / `0X`.
pub fn parse_bytes(s: &str) -> Result<Vec<u8>, String> {
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        if hex.is_empty() {
            return Ok(Vec::new());
        }
        if hex.len() % 2 != 0 {
            return Err(format!("hex value {s:?} has odd length"));
        }
        if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(format!("hex value {s:?} has non-hex digits"));
        }
        (0..hex.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&hex[i..i + 2], 16)
                    .map_err(|_| format!("hex value {s:?} is invalid"))
            })
            .collect()
    } else {
        Ok(s.as_bytes().to_vec())
    }
}

fn parse_ok_or_error(obj: &Map<String, Value>, line_no: usize) -> Result<OpResult, String> {
    match require_field(obj, "result", line_no)? {
        Value::String(s) if s == "ok" => Ok(OpResult::Ok),
        Value::Object(o) => parse_error_object(o, line_no),
        other => Err(format!(
            "line {line_no}: put/delete result must be \"ok\" or {{\"error\":\"...\"}}, got {other}"
        )),
    }
}

fn parse_get_result(obj: &Map<String, Value>, line_no: usize) -> Result<OpResult, String> {
    match require_field(obj, "result", line_no)? {
        Value::Null => Ok(OpResult::Value(None)),
        Value::String(s) => {
            let bytes = parse_bytes(s).map_err(|e| format!("line {line_no}: result: {e}"))?;
            Ok(OpResult::Value(Some(bytes)))
        }
        Value::Object(o) => parse_error_object(o, line_no),
        other => Err(format!(
            "line {line_no}: get result must be a string, null, or {{\"error\":\"...\"}}, got {other}"
        )),
    }
}

fn parse_scan_result(obj: &Map<String, Value>, line_no: usize) -> Result<OpResult, String> {
    match require_field(obj, "result", line_no)? {
        Value::Array(items) => {
            let mut pairs = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                let pair = item.as_array().ok_or_else(|| {
                    format!("line {line_no}: scan result[{i}] must be a [key, value] array")
                })?;
                if pair.len() != 2 {
                    return Err(format!(
                        "line {line_no}: scan result[{i}] must have exactly 2 elements"
                    ));
                }
                let k = pair[0].as_str().ok_or_else(|| {
                    format!("line {line_no}: scan result[{i}] key must be a string")
                })?;
                let v = pair[1].as_str().ok_or_else(|| {
                    format!("line {line_no}: scan result[{i}] value must be a string")
                })?;
                let key = parse_bytes(k)
                    .map_err(|e| format!("line {line_no}: scan result[{i}] key: {e}"))?;
                let val = parse_bytes(v)
                    .map_err(|e| format!("line {line_no}: scan result[{i}] value: {e}"))?;
                pairs.push((key, val));
            }
            Ok(OpResult::Scan(pairs))
        }
        Value::Object(o) => parse_error_object(o, line_no),
        other => Err(format!(
            "line {line_no}: scan result must be an array of [key, value] pairs or {{\"error\":\"...\"}}, got {other}"
        )),
    }
}

fn parse_error_object(obj: &Map<String, Value>, line_no: usize) -> Result<OpResult, String> {
    if obj.len() != 1 || !obj.contains_key("error") {
        return Err(format!(
            "line {line_no}: error result must be {{\"error\":\"...\"}} with no other fields"
        ));
    }
    let msg = obj["error"]
        .as_str()
        .ok_or_else(|| format!("line {line_no}: error result must be a string"))?;
    Ok(OpResult::Error(msg.to_owned()))
}

/// Render a WGL report as machine JSON (pretty-printed).
pub fn report_json(
    ops: usize,
    greedy: Option<&MinimalCounterexample>,
    muss: Option<&[MinimalCounterexample]>,
) -> String {
    let linearizable = greedy.is_none();
    let mut root = serde_json::Map::new();
    root.insert("ops".into(), Value::from(ops));
    root.insert("linearizable".into(), Value::from(linearizable));
    root.insert("greedy".into(), greedy.map(cex_json).unwrap_or(Value::Null));
    if let Some(list) = muss {
        root.insert(
            "mus".into(),
            Value::Array(list.iter().map(cex_json).collect()),
        );
        root.insert("mus_count".into(), Value::from(list.len()));
    }
    Value::Object(root).to_string()
}

fn cex_json(cex: &MinimalCounterexample) -> Value {
    let ops: Vec<Value> = cex
        .original_indices
        .iter()
        .zip(cex.ops.iter())
        .map(|(orig, e)| entry_json(*orig, e))
        .collect();
    let mut edges = Vec::new();
    for (i, a) in cex.ops.iter().enumerate() {
        for (j, b) in cex.ops.iter().enumerate() {
            if i != j && a.end_tick < b.start_tick {
                edges.push(Value::String(format!("{i}≺{j}")));
            }
        }
    }
    serde_json::json!({
        "original_indices": cex.original_indices,
        "ops": ops,
        "real_time_order": edges,
        "why": cex.why,
    })
}

fn entry_json(orig: usize, e: &HistoryEntry) -> Value {
    let mut m = serde_json::Map::new();
    m.insert("index".into(), Value::from(orig));
    if let Some(c) = e.client_id {
        m.insert("client".into(), Value::from(c));
    }
    m.insert("start".into(), Value::from(e.start_tick));
    m.insert("end".into(), Value::from(e.end_tick));
    match &e.op {
        Op::Put { key, value } => {
            m.insert("op".into(), Value::from("put"));
            m.insert("key".into(), Value::from(bytes_to_json(key)));
            m.insert("value".into(), Value::from(bytes_to_json(value)));
        }
        Op::Get { key } => {
            m.insert("op".into(), Value::from("get"));
            m.insert("key".into(), Value::from(bytes_to_json(key)));
        }
        Op::Delete { key } => {
            m.insert("op".into(), Value::from("delete"));
            m.insert("key".into(), Value::from(bytes_to_json(key)));
        }
        Op::Scan { prefix } => {
            m.insert("op".into(), Value::from("scan"));
            m.insert("prefix".into(), Value::from(bytes_to_json(prefix)));
        }
    }
    m.insert("result".into(), result_json(&e.result));
    Value::Object(m)
}

fn result_json(r: &OpResult) -> Value {
    match r {
        OpResult::Ok => Value::from("ok"),
        OpResult::Value(None) => Value::Null,
        OpResult::Value(Some(v)) => Value::from(bytes_to_json(v)),
        OpResult::Scan(items) => Value::Array(
            items
                .iter()
                .map(|(k, v)| {
                    Value::Array(vec![
                        Value::from(bytes_to_json(k)),
                        Value::from(bytes_to_json(v)),
                    ])
                })
                .collect(),
        ),
        OpResult::Error(e) => serde_json::json!({"error": e}),
    }
}

fn bytes_to_json(b: &[u8]) -> String {
    match std::str::from_utf8(b) {
        Ok(s) if !s.starts_with("0x") && !s.starts_with("0X") => s.to_owned(),
        _ => format!("0x{}", crate::linear::hex_enc(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::linear::WGL_MAX_OPS;

    #[test]
    fn parse_put_get_delete_scan_and_null_miss() {
        let jsonl = r#"
{"client":0,"start":1,"end":2,"op":"put","key":"k","value":"v","result":"ok"}
{"client":1,"start":1,"end":3,"op":"get","key":"k","result":"v"}
{"op":"get","key":"missing","result":null}
{"op":"delete","key":"k","result":"ok"}
{"op":"scan","prefix":"a","result":[["a1","v"]]}
"#;
        let checker = parse_history_jsonl(jsonl).expect("parse");
        assert_eq!(checker.len(), 5);
        let e = checker.entries();
        assert_eq!(e[0].client_id, Some(0));
        assert_eq!(e[0].start_tick, 1);
        assert_eq!(e[0].end_tick, 2);
        assert_eq!(
            e[0].op,
            Op::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }
        );
        assert_eq!(e[0].result, OpResult::Ok);
        assert_eq!(e[1].result, OpResult::Value(Some(b"v".to_vec())));
        assert_eq!(e[2].result, OpResult::Value(None));
        assert_eq!(e[3].op, Op::Delete { key: b"k".to_vec() });
        match &e[4].op {
            Op::Scan { prefix } => assert_eq!(prefix, b"a"),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            e[4].result,
            OpResult::Scan(vec![(b"a1".to_vec(), b"v".to_vec())])
        );
    }

    #[test]
    fn parse_hex_keys_and_error_result() {
        let jsonl = r#"
{"op":"put","key":"0x6b","value":"0x76","result":"ok"}
{"op":"get","key":"0X6B","result":{"error":"not leader"}}
"#;
        let checker = parse_history_jsonl(jsonl).unwrap();
        let e = checker.entries();
        assert_eq!(
            e[0].op,
            Op::Put {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
            }
        );
        assert_eq!(e[1].result, OpResult::Error("not leader".into()));
    }

    #[test]
    fn parse_rejects_unknown_field_and_op() {
        let err =
            parse_history_jsonl(r#"{"op":"put","key":"k","value":"v","result":"ok","extra":1}"#)
                .unwrap_err();
        assert!(err.contains("unknown field"), "{err}");
        let err = parse_history_jsonl(r#"{"op":"cas","key":"k","result":"ok"}"#).unwrap_err();
        assert!(err.contains("unknown op"), "{err}");
    }

    #[test]
    fn parse_rejects_missing_result_and_bad_hex() {
        let err = parse_history_jsonl(r#"{"op":"put","key":"k","value":"v"}"#).unwrap_err();
        assert!(err.contains("missing field \"result\""), "{err}");
        let err = parse_history_jsonl(r#"{"op":"get","key":"0xzz","result":null}"#).unwrap_err();
        assert!(err.contains("non-hex"), "{err}");
        let err = parse_history_jsonl(r#"{"op":"get","key":"0xabc","result":null}"#).unwrap_err();
        assert!(err.contains("odd length"), "{err}");
    }

    #[test]
    fn parse_rejects_inverted_interval() {
        let err = parse_history_jsonl(r#"{"op":"get","key":"k","start":5,"end":5,"result":null}"#)
            .unwrap_err();
        assert!(err.contains("start (5) must be < end (5)"), "{err}");
    }

    #[test]
    fn parse_auto_ticks_when_interval_omitted() {
        let jsonl = r#"
{"op":"put","key":"k","value":"v","result":"ok"}
{"op":"get","key":"k","result":"v"}
"#;
        let checker = parse_history_jsonl(jsonl).unwrap();
        let e = checker.entries();
        assert_eq!((e[0].start_tick, e[0].end_tick), (0, 1));
        assert_eq!((e[1].start_tick, e[1].end_tick), (2, 3));
    }

    #[test]
    fn report_json_and_greedy_on_put_then_miss() {
        let jsonl = r#"
{"client":1,"start":0,"end":1,"op":"put","key":"k","value":"v","result":"ok"}
{"client":2,"start":2,"end":3,"op":"get","key":"k","result":null}
"#;
        let checker = parse_history_jsonl(jsonl).unwrap();
        assert!(checker.check_concurrent().is_err());
        let greedy = checker.minimal_counterexample().expect("cex");
        assert_eq!(greedy.ops.len(), 2);
        let muss = checker.minimal_unsatisfiable_subsets(WGL_MAX_OPS);
        assert_eq!(muss.len(), 1);
        let json = report_json(checker.len(), Some(&greedy), Some(&muss));
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["linearizable"], false);
        assert_eq!(v["ops"], 2);
        assert_eq!(v["mus_count"], 1);
        assert_eq!(v["greedy"]["original_indices"], serde_json::json!([0, 1]));
        assert!(v["greedy"]["why"].is_array());
        assert_eq!(v["mus"][0]["ops"][1]["op"], "get");
        assert_eq!(v["mus"][0]["ops"][1]["result"], Value::Null);
    }

    #[test]
    fn report_json_linearizable() {
        let jsonl = r#"{"op":"put","key":"k","value":"v","result":"ok"}
{"op":"get","key":"k","result":"v"}"#;
        let checker = parse_history_jsonl(jsonl).unwrap();
        let json = report_json(checker.len(), None, Some(&[]));
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["linearizable"], true);
        assert_eq!(v["greedy"], Value::Null);
        assert_eq!(v["mus_count"], 0);
    }
}
