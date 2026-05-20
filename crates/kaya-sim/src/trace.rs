// Minimal hand-rolled JSONL trace for the deterministic simulator.
// No external JSON crate is used; the format is simple enough for
// straightforward string matching during replay parsing.
//
// Enum variants carry all fields for completeness; not every field is consumed
// in the replay path but they are part of the documented trace schema.
#![allow(dead_code)]

// ── Hex helpers ──────────────────────────────────────────────────────────────

pub(crate) fn hex_enc(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn hex_dec(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    s.as_bytes()
        .chunks(2)
        .map(|ch| {
            let hi = (ch[0] as char).to_digit(16)? as u8;
            let lo = (ch[1] as char).to_digit(16)? as u8;
            Some(hi << 4 | lo)
        })
        .collect()
}

// ── TraceWriter ───────────────────────────────────────────────────────────────

pub(crate) struct TraceWriter {
    lines: Vec<String>,
    next_eid: u64,
}

impl TraceWriter {
    pub(crate) fn new() -> Self {
        Self {
            lines: Vec::new(),
            next_eid: 1,
        }
    }

    fn push(&mut self, line: String) {
        self.lines.push(line);
    }

    fn eid(&mut self) -> u64 {
        let e = self.next_eid;
        self.next_eid += 1;
        e
    }

    pub(crate) fn sim_start(&mut self, seed: u64, max_ops: u64) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"sim_start","seed":"0x{seed:016x}","max_ops":{max_ops}}}"#
        ));
    }

    pub(crate) fn op_put(&mut self, oid: u64, key: &[u8], value: &[u8]) {
        let e = self.eid();
        let k = hex_enc(key);
        let v = hex_enc(value);
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"put","key":"{k}","val":"{v}"}}"#
        ));
    }

    pub(crate) fn op_get(&mut self, oid: u64, key: &[u8]) {
        let e = self.eid();
        let k = hex_enc(key);
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"get","key":"{k}"}}"#
        ));
    }

    pub(crate) fn op_delete(&mut self, oid: u64, key: &[u8]) {
        let e = self.eid();
        let k = hex_enc(key);
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"delete","key":"{k}"}}"#
        ));
    }

    pub(crate) fn op_scan(&mut self, oid: u64, prefix: &[u8]) {
        let e = self.eid();
        let p = hex_enc(prefix);
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"scan","prefix":"{p}"}}"#
        ));
    }

    pub(crate) fn op_flush(&mut self, oid: u64) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"flush"}}"#
        ));
    }

    pub(crate) fn op_compact(&mut self, oid: u64) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"compact"}}"#
        ));
    }

    pub(crate) fn op_crash_restart(&mut self, oid: u64) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"op","oid":{oid},"cmd":"crash_restart"}}"#
        ));
    }

    pub(crate) fn result_ok(&mut self, oid: u64) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"op_result","oid":{oid},"ok":true}}"#
        ));
    }

    pub(crate) fn result_get(&mut self, oid: u64, value: Option<&[u8]>) {
        let e = self.eid();
        match value {
            Some(v) => {
                let vh = hex_enc(v);
                self.push(format!(
                    r#"{{"eid":{e},"kind":"op_result","oid":{oid},"ok":true,"val":"{vh}"}}"#
                ));
            }
            None => {
                self.push(format!(
                    r#"{{"eid":{e},"kind":"op_result","oid":{oid},"ok":true,"val":null}}"#
                ));
            }
        }
    }

    pub(crate) fn result_scan(&mut self, oid: u64, count: usize) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"op_result","oid":{oid},"ok":true,"count":{count}}}"#
        ));
    }

    pub(crate) fn crash_event(&mut self) {
        let e = self.eid();
        self.push(format!(r#"{{"eid":{e},"kind":"crash"}}"#));
    }

    pub(crate) fn restart_event(&mut self) {
        let e = self.eid();
        self.push(format!(r#"{{"eid":{e},"kind":"restart"}}"#));
    }

    pub(crate) fn invariant_ok(&mut self, id: &str) {
        let e = self.eid();
        self.push(format!(
            r#"{{"eid":{e},"kind":"invariant","id":"{id}","ok":true}}"#
        ));
    }

    pub(crate) fn invariant_violation(&mut self, id: &str, detail: &str) {
        let e = self.eid();
        let safe = detail.replace('"', "'");
        self.push(format!(
            r#"{{"eid":{e},"kind":"violation","id":"{id}","detail":"{safe}"}}"#
        ));
    }

    pub(crate) fn finish(self) -> String {
        self.lines.join("\n")
    }
}

// ── Trace parser (for replay) ─────────────────────────────────────────────────

/// Extract the value of a JSON string field: `"key":"value"` → `"value"`.
fn field_str<'a>(json: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("\"{}\":\"", key);
    let start = json.find(&needle)? + needle.len();
    let after = &json[start..];
    let end = after.find('"')?;
    Some(&after[..end])
}

/// Extract the value of a JSON integer field: `"key":123` → `123`.
fn field_u64(json: &str, key: &str) -> Option<u64> {
    let needle = format!("\"{}\":", key);
    let start = json.find(&needle)? + needle.len();
    let after = &json[start..];
    if after.starts_with('"') || after.starts_with("null") {
        return None;
    }
    let end = after
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(after.len());
    after[..end].parse().ok()
}

fn field_usize(json: &str, key: &str) -> Option<usize> {
    field_u64(json, key).map(|v| v as usize)
}

fn field_hex(json: &str, key: &str) -> Option<Vec<u8>> {
    hex_dec(field_str(json, key)?)
}

fn field_is_null(json: &str, key: &str) -> bool {
    // Match `"key":null` followed by , or }
    let needle = format!("\"{}\":null", key);
    if let Some(pos) = json.find(&needle) {
        let after = &json[pos + needle.len()..];
        let next = after.chars().next().unwrap_or('}');
        return next == ',' || next == '}' || next == ' ';
    }
    false
}

#[derive(Debug, Clone)]
pub(crate) enum ParsedOp {
    Put {
        oid: u64,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    Get {
        oid: u64,
        key: Vec<u8>,
    },
    Delete {
        oid: u64,
        key: Vec<u8>,
    },
    Scan {
        oid: u64,
        prefix: Vec<u8>,
    },
    Flush {
        oid: u64,
    },
    Compact {
        oid: u64,
    },
    CrashRestart {
        oid: u64,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ParsedResult {
    Void { oid: u64 },
    Get { oid: u64, value: Option<Vec<u8>> },
    Scan { oid: u64, count: usize },
}

#[derive(Debug, Clone)]
pub(crate) enum TraceLine {
    SimStart { seed: u64, max_ops: u64 },
    Op(ParsedOp),
    OpResult(ParsedResult),
    Crash,
    Restart,
    InvariantOk,
    Violation { id: String, detail: String },
    Unknown,
}

pub(crate) fn parse_trace(jsonl: &str) -> Vec<TraceLine> {
    jsonl.lines().map(parse_line).collect()
}

fn parse_line(line: &str) -> TraceLine {
    let Some(kind) = field_str(line, "kind") else {
        return TraceLine::Unknown;
    };
    match kind {
        "sim_start" => {
            let seed = field_str(line, "seed")
                .and_then(|s| {
                    let s = s.trim_start_matches("0x").trim_start_matches("0X");
                    u64::from_str_radix(s, 16).ok()
                })
                .unwrap_or(0);
            let max_ops = field_u64(line, "max_ops").unwrap_or(0);
            TraceLine::SimStart { seed, max_ops }
        }
        "op" => {
            let oid = field_u64(line, "oid").unwrap_or(0);
            let Some(cmd) = field_str(line, "cmd") else {
                return TraceLine::Unknown;
            };
            let op = match cmd {
                "put" => ParsedOp::Put {
                    oid,
                    key: field_hex(line, "key").unwrap_or_default(),
                    val: field_hex(line, "val").unwrap_or_default(),
                },
                "get" => ParsedOp::Get {
                    oid,
                    key: field_hex(line, "key").unwrap_or_default(),
                },
                "delete" => ParsedOp::Delete {
                    oid,
                    key: field_hex(line, "key").unwrap_or_default(),
                },
                "scan" => ParsedOp::Scan {
                    oid,
                    prefix: field_hex(line, "prefix").unwrap_or_default(),
                },
                "flush" => ParsedOp::Flush { oid },
                "compact" => ParsedOp::Compact { oid },
                "crash_restart" => ParsedOp::CrashRestart { oid },
                _ => return TraceLine::Unknown,
            };
            TraceLine::Op(op)
        }
        "op_result" => {
            let oid = field_u64(line, "oid").unwrap_or(0);
            if line.contains("\"count\":") {
                let count = field_usize(line, "count").unwrap_or(0);
                TraceLine::OpResult(ParsedResult::Scan { oid, count })
            } else if line.contains("\"val\":") {
                let value = if field_is_null(line, "val") {
                    None
                } else {
                    field_hex(line, "val")
                };
                TraceLine::OpResult(ParsedResult::Get { oid, value })
            } else {
                TraceLine::OpResult(ParsedResult::Void { oid })
            }
        }
        "crash" => TraceLine::Crash,
        "restart" => TraceLine::Restart,
        "invariant" => TraceLine::InvariantOk,
        "violation" => {
            let id = field_str(line, "id").unwrap_or("").to_owned();
            let detail = field_str(line, "detail").unwrap_or("").to_owned();
            TraceLine::Violation { id, detail }
        }
        _ => TraceLine::Unknown,
    }
}
