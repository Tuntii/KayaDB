//! CDC changefeed foundation (M19).
//!
//! Records user-visible put/delete events after a successful WAL append, appends
//! them to a durable JSONL file sink under `cdc/log.jsonl`, and exposes per-consumer
//! cursors with at-least-once poll semantics.
//!
//! This is an **engine-local** foundation (not yet Raft-log based, no TCP sink,
//! no leader-failover chaos proof). See `spec/docs/cdc-spec.md`.

use std::collections::HashMap;

use kaya_core::{Bytes, KayaError, Result};
use kaya_io::{Disk, RelativePath};

use super::Engine;

/// Relative path of the append-only change log (JSONL).
pub const CDC_LOG_PATH: &str = "cdc/log.jsonl";
/// Directory for durable per-consumer cursor files.
pub const CDC_CURSORS_DIR: &str = "cdc/cursors";

const LOG_VERSION: u8 = 1;

/// Change operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcOp {
    Put,
    Delete,
}

impl CdcOp {
    fn parse(s: &str) -> Result<Self> {
        match s {
            "put" => Ok(Self::Put),
            "delete" => Ok(Self::Delete),
            other => Err(KayaError::corruption(format!(
                "unknown cdc op {other:?}"
            ))),
        }
    }
}

/// A single change event emitted after a durable user put/delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcEvent {
    /// Engine sequence number of the primary write (WAL sequence).
    pub seq: u64,
    pub key: Bytes,
    /// `Some` for put; `None` for delete.
    pub value: Option<Bytes>,
    pub op: CdcOp,
}

/// Resumable consumer cursor. `last_seq` is the highest sequence already delivered
/// to this consumer (0 means nothing delivered yet). Poll returns `seq > last_seq`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdcCursor {
    pub consumer_id: String,
    pub last_seq: u64,
}

/// In-memory CDC state loaded from / appended to the file sink.
#[derive(Debug, Default)]
pub(crate) struct CdcState {
    /// Events ordered by sequence ascending.
    pub events: Vec<CdcEvent>,
    /// Last polled (or restored) sequence per consumer id.
    pub consumer_last_seq: HashMap<String, u64>,
}

impl CdcState {
    pub fn new() -> Self {
        Self::default()
    }
}

fn validate_consumer_id(id: &str) -> Result<()> {
    if id.is_empty() || id.len() > 64 {
        return Err(KayaError::invalid_argument(
            "cdc consumer_id must be 1..=64 bytes",
        ));
    }
    if !id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(KayaError::invalid_argument(
            "cdc consumer_id must be ASCII alphanumeric, '_' or '-'",
        ));
    }
    Ok(())
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(s: &str) -> Result<Bytes> {
    if s.len() % 2 != 0 {
        return Err(KayaError::corruption("cdc hex length not even"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_nibble(bytes[i])?;
        let lo = hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(KayaError::corruption("invalid cdc hex digit")),
    }
}

/// Encode one event as a single JSONL line (no serde dependency).
pub(crate) fn encode_event_line(ev: &CdcEvent) -> String {
    match ev.op {
        CdcOp::Put => {
            let value = ev.value.as_deref().unwrap_or(&[]);
            format!(
                "{{\"v\":{},\"seq\":{},\"op\":\"put\",\"key\":\"{}\",\"value\":\"{}\"}}\n",
                LOG_VERSION,
                ev.seq,
                hex_encode(&ev.key),
                hex_encode(value)
            )
        }
        CdcOp::Delete => format!(
            "{{\"v\":{},\"seq\":{},\"op\":\"delete\",\"key\":\"{}\"}}\n",
            LOG_VERSION,
            ev.seq,
            hex_encode(&ev.key)
        ),
    }
}

/// Parse one JSONL line into a [`CdcEvent`]. Blank lines are skipped by the caller.
pub(crate) fn decode_event_line(line: &str) -> Result<CdcEvent> {
    let line = line.trim();
    if !line.starts_with('{') || !line.ends_with('}') {
        return Err(KayaError::corruption("cdc log line is not a JSON object"));
    }
    let body = &line[1..line.len() - 1];
    let mut version: Option<u8> = None;
    let mut seq: Option<u64> = None;
    let mut op: Option<CdcOp> = None;
    let mut key: Option<Bytes> = None;
    let mut value: Option<Option<Bytes>> = None;

    for field in split_json_fields(body)? {
        let (k, v) = field;
        match k.as_str() {
            "v" => {
                version = Some(
                    v.parse::<u8>()
                        .map_err(|_| KayaError::corruption("cdc bad version"))?,
                );
            }
            "seq" => {
                seq = Some(
                    v.parse::<u64>()
                        .map_err(|_| KayaError::corruption("cdc bad seq"))?,
                );
            }
            "op" => {
                op = Some(CdcOp::parse(&v)?);
            }
            "key" => {
                key = Some(hex_decode(&v)?);
            }
            "value" => {
                value = Some(Some(hex_decode(&v)?));
            }
            _ => {}
        }
    }

    let version = version.ok_or_else(|| KayaError::corruption("cdc missing v"))?;
    if version != LOG_VERSION {
        return Err(KayaError::corruption(format!(
            "unsupported cdc log version {version}"
        )));
    }
    let seq = seq.ok_or_else(|| KayaError::corruption("cdc missing seq"))?;
    let op = op.ok_or_else(|| KayaError::corruption("cdc missing op"))?;
    let key = key.ok_or_else(|| KayaError::corruption("cdc missing key"))?;
    let value = match op {
        CdcOp::Put => value.unwrap_or(Some(Vec::new())),
        CdcOp::Delete => None,
    };

    Ok(CdcEvent {
        seq,
        key,
        value,
        op,
    })
}

/// Minimal field splitter for our fixed-shape JSON objects (no nested objects).
fn split_json_fields(body: &str) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut rest = body.trim();
    while !rest.is_empty() {
        rest = rest.trim_start();
        if !rest.starts_with('"') {
            return Err(KayaError::corruption("cdc expected field name"));
        }
        let (name, after_name) = parse_json_string(rest)?;
        rest = after_name.trim_start();
        if !rest.starts_with(':') {
            return Err(KayaError::corruption("cdc expected ':' after field name"));
        }
        rest = rest[1..].trim_start();
        let (value, after_val) = if rest.starts_with('"') {
            let (s, after) = parse_json_string(rest)?;
            (s, after)
        } else {
            // number
            let end = rest
                .find(|c: char| c == ',' || c.is_whitespace())
                .unwrap_or(rest.len());
            (rest[..end].to_string(), &rest[end..])
        };
        out.push((name, value));
        rest = after_val.trim_start();
        if rest.starts_with(',') {
            rest = rest[1..].trim_start();
        } else if rest.is_empty() {
            break;
        } else {
            return Err(KayaError::corruption("cdc unexpected trailing field data"));
        }
    }
    Ok(out)
}

fn parse_json_string(s: &str) -> Result<(String, &str)> {
    if !s.starts_with('"') {
        return Err(KayaError::corruption("cdc expected string"));
    }
    let bytes = s.as_bytes();
    let mut i = 1;
    let mut out = String::new();
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Ok((out, &s[i + 1..])),
            b'\\' => {
                i += 1;
                if i >= bytes.len() {
                    return Err(KayaError::corruption("cdc truncated escape"));
                }
                // Only the escapes we emit (none beyond plain hex content).
                out.push(bytes[i] as char);
                i += 1;
            }
            b => {
                out.push(b as char);
                i += 1;
            }
        }
    }
    Err(KayaError::corruption("cdc unterminated string"))
}

fn cursor_rel_path(consumer_id: &str) -> Result<RelativePath> {
    RelativePath::new(&format!("{CDC_CURSORS_DIR}/{consumer_id}"))
}

fn log_rel_path() -> Result<RelativePath> {
    RelativePath::new(CDC_LOG_PATH)
}

async fn read_cursor_file<D: Disk>(disk: &D, consumer_id: &str) -> Result<u64> {
    let path = cursor_rel_path(consumer_id)?;
    let len = disk.file_len(&path).await?;
    if len == 0 {
        return Ok(0);
    }
    let mut buf = vec![0u8; len as usize];
    let mut offset = 0u64;
    while offset < len {
        let n = disk
            .read_at(&path, offset, &mut buf[offset as usize..])
            .await?;
        if n == 0 {
            break;
        }
        offset += n as u64;
    }
    let s = std::str::from_utf8(&buf[..offset as usize])
        .map_err(|_| KayaError::corruption("cdc cursor not utf-8"))?
        .trim();
    s.parse::<u64>()
        .map_err(|_| KayaError::corruption("cdc cursor not a u64"))
}

async fn load_cdc_log_and_cursors<D: Disk>(disk: &D) -> Result<CdcState> {
    let mut state = CdcState::new();

    // Load log (missing file → empty).
    let log_path = log_rel_path()?;
    match disk.file_len(&log_path).await {
        Ok(len) if len > 0 => {
            let mut buf = vec![0u8; len as usize];
            let mut offset = 0u64;
            while offset < len {
                let n = disk
                    .read_at(&log_path, offset, &mut buf[offset as usize..])
                    .await?;
                if n == 0 {
                    break;
                }
                offset += n as u64;
            }
            let text = String::from_utf8_lossy(&buf[..offset as usize]);
            for line in text.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                state.events.push(decode_event_line(line)?);
            }
            // Ensure order by seq (file is append-only so already ordered).
            state.events.sort_by_key(|e| e.seq);
        }
        Ok(_) | Err(KayaError::NotFound) => {}
        Err(e) => return Err(e),
    }

    // Load cursor directory (best-effort).
    let cursors_dir = RelativePath::new(CDC_CURSORS_DIR)?;
    match disk.list_dir(&cursors_dir).await {
        Ok(entries) => {
            for ent in entries {
                if ent.is_dir {
                    continue;
                }
                let name = ent
                    .path
                    .as_str()
                    .rsplit('/')
                    .next()
                    .unwrap_or("")
                    .to_string();
                if name.is_empty() || validate_consumer_id(&name).is_err() {
                    continue;
                }
                if let Ok(seq) = read_cursor_file(disk, &name).await {
                    state.consumer_last_seq.insert(name, seq);
                }
            }
        }
        Err(KayaError::NotFound) => {}
        Err(e) => return Err(e),
    }

    Ok(state)
}

impl<D: Disk> Engine<D> {
    /// Load durable CDC log + cursor files into memory (called from `open` when enabled).
    pub(crate) async fn load_cdc_state(&mut self) -> Result<()> {
        if !self.config.enable_cdc {
            self.cdc = CdcState::new();
            return Ok(());
        }
        // Clone Arc so we do not hold `&mut Engine` (or `&Engine`) across awaits;
        // `Engine` is intentionally `!Sync` (block cache `RefCell`).
        let disk = self.disk.clone();
        self.cdc = load_cdc_log_and_cursors(disk.as_ref()).await?;
        Ok(())
    }

    /// Append a change event to the in-memory log and durable file sink.
    pub(crate) async fn append_cdc_event(&mut self, event: CdcEvent) -> Result<()> {
        if !self.config.enable_cdc {
            return Ok(());
        }
        let line = encode_event_line(&event);
        let path = log_rel_path()?;
        self.disk.append(&path, line.as_bytes()).await?;
        // Durable enough for foundation tests; full durability policy later.
        self.disk.fsync_file(&path).await?;
        self.cdc.events.push(event);
        Ok(())
    }

    /// Subscribe a consumer. `from_seq` overrides the durable checkpoint when set
    /// (poll delivers events with `seq > from_seq`). When `None`, uses the last
    /// checkpointed / polled sequence for this consumer (or 0).
    pub fn cdc_subscribe(
        &self,
        consumer_id: &str,
        from_seq: Option<u64>,
    ) -> Result<CdcCursor> {
        if !self.config.enable_cdc {
            return Err(KayaError::invalid_argument(
                "cdc is disabled (EngineConfig.enable_cdc = false)",
            ));
        }
        validate_consumer_id(consumer_id)?;
        let last_seq = match from_seq {
            Some(s) => s,
            None => self
                .cdc
                .consumer_last_seq
                .get(consumer_id)
                .copied()
                .unwrap_or(0),
        };
        Ok(CdcCursor {
            consumer_id: consumer_id.to_string(),
            last_seq,
        })
    }

    /// Poll up to `limit` events after `cursor.last_seq`. Advances the cursor and
    /// the engine's in-memory consumer position. Delivery is **at-least-once**:
    /// without a durable [`Self::cdc_checkpoint`], reopen may redeliver.
    ///
    /// Per-key order follows global sequence order (monotone per key).
    pub fn cdc_poll(
        &mut self,
        cursor: &mut CdcCursor,
        limit: usize,
    ) -> Result<Vec<CdcEvent>> {
        if !self.config.enable_cdc {
            return Err(KayaError::invalid_argument(
                "cdc is disabled (EngineConfig.enable_cdc = false)",
            ));
        }
        validate_consumer_id(&cursor.consumer_id)?;
        if limit == 0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for ev in &self.cdc.events {
            if ev.seq <= cursor.last_seq {
                continue;
            }
            out.push(ev.clone());
            if out.len() >= limit {
                break;
            }
        }
        if let Some(last) = out.last() {
            cursor.last_seq = last.seq;
            self.cdc
                .consumer_last_seq
                .insert(cursor.consumer_id.clone(), cursor.last_seq);
        }
        Ok(out)
    }

    /// Persist the consumer's last polled sequence under `cdc/cursors/{id}`.
    ///
    /// Incremental `kayactl backup` remains file-tree based today; a future
    /// enhancement can use these checkpoints as CDC-aware backup watermarks.
    pub async fn cdc_checkpoint(&mut self, consumer_id: &str) -> Result<()> {
        if !self.config.enable_cdc {
            return Err(KayaError::invalid_argument(
                "cdc is disabled (EngineConfig.enable_cdc = false)",
            ));
        }
        validate_consumer_id(consumer_id)?;
        let last_seq = self
            .cdc
            .consumer_last_seq
            .get(consumer_id)
            .copied()
            .unwrap_or(0);
        let path = cursor_rel_path(consumer_id)?;
        let payload = format!("{last_seq}\n");
        // Atomic-ish: write then fsync (full rename publish can come later).
        self.disk.write_at(&path, 0, payload.as_bytes()).await?;
        // Truncate any previous longer content.
        self.disk.truncate(&path, payload.len() as u64).await?;
        self.disk.fsync_file(&path).await?;
        Ok(())
    }

    /// Number of events currently held in the in-memory CDC log (tests / stats).
    pub fn cdc_event_count(&self) -> usize {
        self.cdc.events.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use kaya_core::{DurabilityMode, EngineConfig};
    use kaya_io::SimDisk;

    use super::*;
    use crate::{Engine, WriteOptions};

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    }

    fn strict_opts() -> WriteOptions {
        WriteOptions {
            durability: Some(DurabilityMode::Strict),
            ..WriteOptions::default()
        }
    }

    #[test]
    fn roundtrip_put_delete_lines() {
        let put = CdcEvent {
            seq: 7,
            key: b"k1".to_vec(),
            value: Some(b"v1".to_vec()),
            op: CdcOp::Put,
        };
        let line = encode_event_line(&put);
        let decoded = decode_event_line(line.trim()).unwrap();
        assert_eq!(decoded, put);

        let del = CdcEvent {
            seq: 8,
            key: b"k1".to_vec(),
            value: None,
            op: CdcOp::Delete,
        };
        let line = encode_event_line(&del);
        let decoded = decode_event_line(line.trim()).unwrap();
        assert_eq!(decoded, del);
    }

    #[test]
    fn put_delete_produce_events_in_order() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            engine
                .put(b"a".to_vec(), b"1".to_vec(), strict_opts())
                .await
                .unwrap();
            engine
                .put(b"b".to_vec(), b"2".to_vec(), strict_opts())
                .await
                .unwrap();
            engine.delete(b"a".to_vec(), strict_opts()).await.unwrap();

            assert_eq!(engine.cdc_event_count(), 3);

            let mut cursor = engine.cdc_subscribe("c1", Some(0)).unwrap();
            let events = engine.cdc_poll(&mut cursor, 10).unwrap();
            assert_eq!(events.len(), 3);
            assert_eq!(events[0].op, CdcOp::Put);
            assert_eq!(events[0].key, b"a");
            assert_eq!(events[0].value.as_deref(), Some(b"1".as_slice()));
            assert_eq!(events[1].op, CdcOp::Put);
            assert_eq!(events[1].key, b"b");
            assert_eq!(events[2].op, CdcOp::Delete);
            assert_eq!(events[2].key, b"a");
            assert!(events[0].seq < events[1].seq);
            assert!(events[1].seq < events[2].seq);
        });
    }

    #[test]
    fn poll_cursor_resumes_at_least_once() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let mut engine = Engine::open(EngineConfig::default(), disk).await.unwrap();

            for i in 0..5u8 {
                engine
                    .put(vec![b'k', i], vec![b'v', i], strict_opts())
                    .await
                    .unwrap();
            }

            let mut cursor = engine.cdc_subscribe("worker", Some(0)).unwrap();
            let batch1 = engine.cdc_poll(&mut cursor, 2).unwrap();
            assert_eq!(batch1.len(), 2);
            let after_first = cursor.last_seq;

            let batch2 = engine.cdc_poll(&mut cursor, 10).unwrap();
            assert_eq!(batch2.len(), 3);
            assert!(batch2[0].seq > after_first);

            // Redeliver from earlier seq is allowed (at-least-once).
            let mut replay = engine.cdc_subscribe("worker", Some(0)).unwrap();
            let all = engine.cdc_poll(&mut replay, 100).unwrap();
            assert_eq!(all.len(), 5);
            assert_eq!(all[0].seq, batch1[0].seq);
        });
    }

    #[test]
    fn reopen_engine_continues_from_log_file() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig::default();

            {
                let mut engine = Engine::open(config.clone(), disk.clone()).await.unwrap();
                engine
                    .put(b"x".to_vec(), b"1".to_vec(), strict_opts())
                    .await
                    .unwrap();
                engine
                    .put(b"y".to_vec(), b"2".to_vec(), strict_opts())
                    .await
                    .unwrap();
                let mut cursor = engine.cdc_subscribe("backup", Some(0)).unwrap();
                let first = engine.cdc_poll(&mut cursor, 1).unwrap();
                assert_eq!(first.len(), 1);
                engine.cdc_checkpoint("backup").await.unwrap();
            }

            let mut engine = Engine::open(config, disk).await.unwrap();
            assert_eq!(engine.cdc_event_count(), 2);

            // Resume from durable checkpoint: only second event.
            let mut cursor = engine.cdc_subscribe("backup", None).unwrap();
            let rest = engine.cdc_poll(&mut cursor, 10).unwrap();
            assert_eq!(rest.len(), 1);
            assert_eq!(rest[0].key, b"y");
            assert_eq!(rest[0].value.as_deref(), Some(b"2".as_slice()));
        });
    }

    #[test]
    fn cdc_disabled_writes_no_events() {
        block_on(async {
            let disk = Arc::new(SimDisk::new());
            let config = EngineConfig {
                enable_cdc: false,
                ..EngineConfig::default()
            };
            let mut engine = Engine::open(config, disk).await.unwrap();
            engine
                .put(b"a".to_vec(), b"1".to_vec(), strict_opts())
                .await
                .unwrap();
            assert_eq!(engine.cdc_event_count(), 0);
            assert!(engine.cdc_subscribe("c", Some(0)).is_err());
        });
    }
}
