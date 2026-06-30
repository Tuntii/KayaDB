//! Structured JSONL audit log for client protocol operations.

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use kaya_raft::NodeId;

/// Append-only audit sink at `{data_dir}/audit.jsonl`.
pub struct AuditLog {
    node_id: u64,
    file: Mutex<File>,
}

impl AuditLog {
    /// Open (or create) the audit log under `data_dir`.
    pub fn open(data_dir: &Path, node_id: NodeId) -> io::Result<Self> {
        let path = data_dir.join("audit.jsonl");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            node_id: node_id.0,
            file: Mutex::new(file),
        })
    }

    /// Record one client protocol operation. Best-effort: I/O errors are ignored.
    pub fn record(
        &self,
        peer: SocketAddr,
        opcode: u8,
        status: u16,
        auth_kind: &str,
        key_len: Option<usize>,
    ) {
        let _ = self.record_inner(peer, opcode, status, auth_kind, key_len);
    }

    fn record_inner(
        &self,
        peer: SocketAddr,
        opcode: u8,
        status: u16,
        auth_kind: &str,
        key_len: Option<usize>,
    ) -> io::Result<()> {
        let line = format_audit_line(
            &utc_timestamp_ms(),
            self.node_id,
            peer,
            opcode,
            status,
            auth_kind,
            key_len,
        );
        let mut guard = self
            .file
            .lock()
            .map_err(|_| io::Error::other("audit log mutex poisoned"))?;
        guard.write_all(line.as_bytes())?;
        // Flush for durability without blocking on fsync.
        let _ = guard.flush();
        Ok(())
    }

}

fn format_audit_line(
    ts: &str,
    node_id: u64,
    peer: SocketAddr,
    opcode: u8,
    status: u16,
    auth_kind: &str,
    key_len: Option<usize>,
) -> String {
    let mut line = match key_len {
        Some(len) => format!(
            r#"{{"ts":"{ts}","node_id":{node_id},"peer":"{peer}","opcode":{opcode},"status":{status},"auth":"{auth_kind}","key_len":{len}}}"#,
        ),
        None => format!(
            r#"{{"ts":"{ts}","node_id":{node_id},"peer":"{peer}","opcode":{opcode},"status":{status},"auth":"{auth_kind}"}}"#,
        ),
    };
    line.push('\n');
    line
}

fn utc_timestamp_ms() -> String {
    let now = SystemTime::now();
    let dur = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    let (year, month, day, hour, min, sec) = unix_secs_to_utc(secs);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{millis:03}Z"
    )
}

/// Convert Unix seconds to UTC calendar components (no external deps).
fn unix_secs_to_utc(mut secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sec = (secs % 60) as u32;
    secs /= 60;
    let min = (secs % 60) as u32;
    secs /= 60;
    let hour = (secs % 24) as u32;
    let days = secs / 86400;

    // Civil date from days since 1970-01-01 (algorithm from Howard Hinnant).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era as i64 * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 {
        mp + 3
    } else {
        mp - 9
    } as u32;
    let year = if month <= 2 {
        (y + 1) as u32
    } else {
        y as u32
    };
    (year, month, day, hour, min, sec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn extract_json_u64(json: &str, key: &str) -> Result<u64, String> {
        let needle = format!("\"{key}\":");
        let start = json.find(&needle).ok_or_else(|| format!("missing {key}"))? + needle.len();
        let rest = json[start..].trim_start();
        let end = rest
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(rest.len());
        rest[..end].parse().map_err(|e| format!("{key}: {e}"))
    }

    fn extract_json_str(json: &str, key: &str) -> Result<String, String> {
        let needle = format!("\"{key}\":\"");
        let start = json.find(&needle).ok_or_else(|| format!("missing {key}"))? + needle.len();
        let rest = &json[start..];
        let end = rest
            .find('"')
            .ok_or_else(|| format!("unterminated {key}"))?;
        Ok(rest[..end].to_owned())
    }

    #[test]
    fn audit_jsonl_has_required_fields() {
        let dir = std::env::temp_dir().join(format!("kaya-audit-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let log = AuditLog::open(&dir, NodeId(1)).unwrap();
        let peer = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 54321));
        log.record(peer, 1, 0, "client", Some(7));

        let contents = std::fs::read_to_string(dir.join("audit.jsonl")).unwrap();
        let line = contents.lines().next().expect("one audit line");
        assert!(line.starts_with('{') && line.ends_with('}'));

        let ts = extract_json_str(line, "ts").unwrap();
        assert!(ts.ends_with('Z'));
        assert!(ts.contains('T'));

        assert_eq!(extract_json_u64(line, "node_id").unwrap(), 1);
        assert_eq!(extract_json_str(line, "peer").unwrap(), "127.0.0.1:54321");
        assert_eq!(extract_json_u64(line, "opcode").unwrap(), 1);
        assert_eq!(extract_json_u64(line, "status").unwrap(), 0);
        assert_eq!(extract_json_str(line, "auth").unwrap(), "client");
        assert_eq!(extract_json_u64(line, "key_len").unwrap(), 7);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn audit_jsonl_omits_key_len_when_not_applicable() {
        let line = format_audit_line(
            "2026-06-30T12:00:00.000Z",
            2,
            "127.0.0.1:7379".parse().unwrap(),
            5,
            0,
            "none",
            None,
        );
        assert!(!line.contains("key_len"));
        assert!(line.contains(r#""auth":"none""#));
    }
}