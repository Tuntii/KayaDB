//! Binary codec for Raft [`Envelope`] messages.
//!
//! Wire format for a single frame:
//!
//! ```text
//! frame_len : u32 LE   (byte count of everything that follows)
//! from_id   : u64 LE
//! to_id     : u64 LE
//! msg_type  : u8       (1 = VoteRequest, 2 = VoteResponse,
//!                       3 = AppendRequest, 4 = AppendResponse)
//! <message-specific fields>
//! ```
//!
//! `AppendRequest` entries are each encoded as:
//! `term(u64) | cmd_len(u32) | cmd_bytes`.

use kaya_raft::{
    AppendRequest, AppendResponse, ConfigChangeRequest, ConfigChangeResponse, Envelope,
    InstallSnapshotRequest, InstallSnapshotResponse, LogEntry, LogIndex, Message, NodeId, Term,
    VoteRequest, VoteResponse,
};

use crate::DEFAULT_MAX_FRAME_LEN;

const MSG_VOTE_REQUEST: u8 = 1;
const MSG_VOTE_RESPONSE: u8 = 2;
const MSG_APPEND_REQUEST: u8 = 3;
const MSG_APPEND_RESPONSE: u8 = 4;
const MSG_INSTALL_SNAPSHOT_REQUEST: u8 = 5;
const MSG_INSTALL_SNAPSHOT_RESPONSE: u8 = 6;
const MSG_CONFIG_CHANGE_REQUEST: u8 = 7;
const MSG_CONFIG_CHANGE_RESPONSE: u8 = 8;

/// Max snapshot payload bytes on the wire (frame budget minus headers).
pub const MAX_SNAPSHOT_DATA_LEN: u32 = DEFAULT_MAX_FRAME_LEN.saturating_sub(64);

// ── tiny write helpers ────────────────────────────────────────────────────────

fn push_u8(buf: &mut Vec<u8>, v: u8) {
    buf.push(v);
}

fn push_u32(buf: &mut Vec<u8>, v: u32) {
    buf.extend_from_slice(&v.to_le_bytes());
}

fn push_u64(buf: &mut Vec<u8>, v: u64) {
    buf.extend_from_slice(&v.to_le_bytes());
}

// ── tiny read helpers ─────────────────────────────────────────────────────────

fn take_u8(cur: &mut &[u8]) -> Result<u8, String> {
    if cur.is_empty() {
        return Err("unexpected EOF reading u8".to_owned());
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, String> {
    if cur.len() < 4 {
        return Err(format!(
            "unexpected EOF reading u32 (have {} bytes)",
            cur.len()
        ));
    }
    let v = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]);
    *cur = &cur[4..];
    Ok(v)
}

fn take_u64(cur: &mut &[u8]) -> Result<u64, String> {
    if cur.len() < 8 {
        return Err(format!(
            "unexpected EOF reading u64 (have {} bytes)",
            cur.len()
        ));
    }
    let bytes: [u8; 8] = cur[..8].try_into().unwrap();
    *cur = &cur[8..];
    Ok(u64::from_le_bytes(bytes))
}

fn take_bytes(cur: &mut &[u8], len: usize) -> Result<Vec<u8>, String> {
    if cur.len() < len {
        return Err(format!(
            "truncated data: need {len} bytes, have {}",
            cur.len()
        ));
    }
    let v = cur[..len].to_vec();
    *cur = &cur[len..];
    Ok(v)
}

// ── public API ────────────────────────────────────────────────────────────────

/// Encode an [`Envelope`] to a length-prefixed byte frame.
pub fn encode_envelope(env: &Envelope) -> Vec<u8> {
    let mut body = Vec::new();
    push_u64(&mut body, env.from.0);
    push_u64(&mut body, env.to.0);
    match &env.message {
        Message::VoteRequest(m) => {
            push_u8(&mut body, MSG_VOTE_REQUEST);
            push_u64(&mut body, m.term.0);
            push_u64(&mut body, m.candidate_id.0);
            push_u64(&mut body, m.last_log_index.0);
            push_u64(&mut body, m.last_log_term.0);
        }
        Message::VoteResponse(m) => {
            push_u8(&mut body, MSG_VOTE_RESPONSE);
            push_u64(&mut body, m.term.0);
            push_u8(&mut body, m.vote_granted as u8);
        }
        Message::AppendRequest(m) => {
            push_u8(&mut body, MSG_APPEND_REQUEST);
            push_u64(&mut body, m.term.0);
            push_u64(&mut body, m.leader_id.0);
            push_u64(&mut body, m.prev_log_index.0);
            push_u64(&mut body, m.prev_log_term.0);
            push_u64(&mut body, m.leader_commit.0);
            push_u32(&mut body, m.entries.len() as u32);
            for entry in &m.entries {
                push_u64(&mut body, entry.term.0);
                push_u32(&mut body, entry.command.len() as u32);
                body.extend_from_slice(&entry.command);
            }
        }
        Message::AppendResponse(m) => {
            push_u8(&mut body, MSG_APPEND_RESPONSE);
            push_u64(&mut body, m.term.0);
            push_u8(&mut body, m.success as u8);
            push_u64(&mut body, m.match_index.0);
        }
        Message::InstallSnapshotRequest(m) => {
            push_u8(&mut body, MSG_INSTALL_SNAPSHOT_REQUEST);
            push_u64(&mut body, m.term.0);
            push_u64(&mut body, m.leader_id.0);
            push_u64(&mut body, m.last_included_index.0);
            push_u64(&mut body, m.last_included_term.0);
            push_u32(&mut body, m.data.len() as u32);
            body.extend_from_slice(&m.data);
        }
        Message::InstallSnapshotResponse(m) => {
            push_u8(&mut body, MSG_INSTALL_SNAPSHOT_RESPONSE);
            push_u64(&mut body, m.term.0);
            push_u8(&mut body, m.success as u8);
        }
        Message::ConfigChangeRequest(m) => {
            push_u8(&mut body, MSG_CONFIG_CHANGE_REQUEST);
            push_u64(&mut body, m.term.0);
            // old_peers: len + ids
            push_u32(&mut body, m.old_peers.len() as u32);
            for p in &m.old_peers {
                push_u64(&mut body, p.0);
            }
            // new_peers
            push_u32(&mut body, m.new_peers.len() as u32);
            for p in &m.new_peers {
                push_u64(&mut body, p.0);
            }
        }
        Message::ConfigChangeResponse(m) => {
            push_u8(&mut body, MSG_CONFIG_CHANGE_RESPONSE);
            push_u64(&mut body, m.term.0);
            push_u8(&mut body, m.success as u8);
        }
    }

    let mut out = Vec::with_capacity(4 + body.len());
    push_u32(&mut out, body.len() as u32);
    out.extend_from_slice(&body);
    out
}

/// Decode an [`Envelope`] from the payload bytes **after** the 4-byte length prefix.
pub fn decode_envelope(data: &[u8]) -> Result<Envelope, String> {
    let mut cur = data;
    let from = NodeId(take_u64(&mut cur)?);
    let to = NodeId(take_u64(&mut cur)?);
    let msg_type = take_u8(&mut cur)?;

    let message = match msg_type {
        MSG_VOTE_REQUEST => Message::VoteRequest(VoteRequest {
            term: Term(take_u64(&mut cur)?),
            candidate_id: NodeId(take_u64(&mut cur)?),
            last_log_index: LogIndex(take_u64(&mut cur)?),
            last_log_term: Term(take_u64(&mut cur)?),
        }),

        MSG_VOTE_RESPONSE => Message::VoteResponse(VoteResponse {
            term: Term(take_u64(&mut cur)?),
            vote_granted: take_u8(&mut cur)? != 0,
        }),

        MSG_APPEND_REQUEST => {
            let term = Term(take_u64(&mut cur)?);
            let leader_id = NodeId(take_u64(&mut cur)?);
            let prev_log_index = LogIndex(take_u64(&mut cur)?);
            let prev_log_term = Term(take_u64(&mut cur)?);
            let leader_commit = LogIndex(take_u64(&mut cur)?);
            let entry_count = take_u32(&mut cur)?;
            if entry_count > 100_000 {
                return Err(format!("suspiciously large entry_count: {entry_count}"));
            }
            let mut entries = Vec::with_capacity(entry_count as usize);
            for _ in 0..entry_count {
                let entry_term = Term(take_u64(&mut cur)?);
                let cmd_len = take_u32(&mut cur)? as usize;
                if cmd_len > 16 * 1024 * 1024 {
                    return Err(format!("command too large: {cmd_len}"));
                }
                let command = take_bytes(&mut cur, cmd_len)?;
                entries.push(LogEntry {
                    term: entry_term,
                    command,
                });
            }
            Message::AppendRequest(AppendRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            })
        }

        MSG_APPEND_RESPONSE => Message::AppendResponse(AppendResponse {
            term: Term(take_u64(&mut cur)?),
            success: take_u8(&mut cur)? != 0,
            match_index: LogIndex(take_u64(&mut cur)?),
        }),

        MSG_INSTALL_SNAPSHOT_REQUEST => {
            let term = Term(take_u64(&mut cur)?);
            let leader_id = NodeId(take_u64(&mut cur)?);
            let last_included_index = LogIndex(take_u64(&mut cur)?);
            let last_included_term = Term(take_u64(&mut cur)?);
            let data_len = take_u32(&mut cur)?;
            if data_len > MAX_SNAPSHOT_DATA_LEN {
                return Err(format!("snapshot data too large: {data_len}"));
            }
            let data = take_bytes(&mut cur, data_len as usize)?;
            Message::InstallSnapshotRequest(InstallSnapshotRequest {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                data,
            })
        }

        MSG_INSTALL_SNAPSHOT_RESPONSE => {
            Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: Term(take_u64(&mut cur)?),
                success: take_u8(&mut cur)? != 0,
            })
        }

        MSG_CONFIG_CHANGE_REQUEST => {
            let term = Term(take_u64(&mut cur)?);
            let old_len = take_u32(&mut cur)? as usize;
            let mut old_peers = Vec::with_capacity(old_len);
            for _ in 0..old_len {
                old_peers.push(NodeId(take_u64(&mut cur)?));
            }
            let new_len = take_u32(&mut cur)? as usize;
            let mut new_peers = Vec::with_capacity(new_len);
            for _ in 0..new_len {
                new_peers.push(NodeId(take_u64(&mut cur)?));
            }
            Message::ConfigChangeRequest(ConfigChangeRequest {
                term,
                old_peers,
                new_peers,
            })
        }

        MSG_CONFIG_CHANGE_RESPONSE => Message::ConfigChangeResponse(ConfigChangeResponse {
            term: Term(take_u64(&mut cur)?),
            success: take_u8(&mut cur)? != 0,
        }),

        t => return Err(format!("unknown Raft message type: {t}")),
    };

    Ok(Envelope { from, to, message })
}

// ── client protocol payload helpers ──────────────────────────────────────────

/// Encode a PUT request payload: `key_len(u32) | value_len(u32) | key | value`.
pub fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + key.len() + value.len());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    out
}

/// Decode a PUT request payload.
pub fn decode_put_payload(data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut cur = data;
    let key_len = take_u32(&mut cur)? as usize;
    let value_len = take_u32(&mut cur)? as usize;
    let key = take_bytes(&mut cur, key_len)?;
    let value = take_bytes(&mut cur, value_len)?;
    Ok((key, value))
}

/// Encode a GET/DELETE request payload: `key_len(u32) | key`.
pub fn encode_key_payload(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + key.len());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out
}

/// Decode a GET/DELETE request payload.
pub fn decode_key_payload(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut cur = data;
    let key_len = take_u32(&mut cur)? as usize;
    take_bytes(&mut cur, key_len)
}

/// Encode ADD_MEMBER payload: `node_id(u64) | raft_len | raft | client_len | client`.
pub fn encode_member_payload(node_id: u64, raft_addr: &str, client_addr: &str) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&node_id.to_le_bytes());
    push_u32(&mut out, raft_addr.len() as u32);
    out.extend_from_slice(raft_addr.as_bytes());
    push_u32(&mut out, client_addr.len() as u32);
    out.extend_from_slice(client_addr.as_bytes());
    out
}

/// Encode REMOVE_MEMBER payload: `node_id(u64)`.
pub fn encode_remove_member_payload(node_id: u64) -> Vec<u8> {
    node_id.to_le_bytes().to_vec()
}

/// Decode REMOVE_MEMBER payload.
pub fn decode_remove_member_payload(data: &[u8]) -> Result<u64, String> {
    if data.len() < 8 {
        return Err("truncated REMOVE_MEMBER payload".to_owned());
    }
    Ok(u64::from_le_bytes([
        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
    ]))
}

/// Decode ADD_MEMBER payload.
pub fn decode_member_payload(data: &[u8]) -> Result<(u64, String, String), String> {
    let mut cur = data;
    let node_id = take_u64(&mut cur)?;
    let raft_len = take_u32(&mut cur)? as usize;
    let raft = String::from_utf8(take_bytes(&mut cur, raft_len)?)
        .map_err(|e| format!("invalid raft addr utf-8: {e}"))?;
    let client_len = take_u32(&mut cur)? as usize;
    let client = String::from_utf8(take_bytes(&mut cur, client_len)?)
        .map_err(|e| format!("invalid client addr utf-8: {e}"))?;
    Ok((node_id, raft, client))
}

/// Encode a SCAN request payload: `prefix_len(u32) | prefix`.
pub fn encode_scan_payload(prefix: &[u8]) -> Vec<u8> {
    encode_key_payload(prefix)
}

/// Decode a SCAN request payload.
pub fn decode_scan_payload(data: &[u8]) -> Result<Vec<u8>, String> {
    decode_key_payload(data)
}

/// Encode a GET OK response payload: `value_len(u32) | value`.
pub fn encode_value_payload(value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + value.len());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
    out
}

/// Decode a GET OK response payload.
pub fn decode_value_payload(data: &[u8]) -> Result<Vec<u8>, String> {
    let mut cur = data;
    let len = take_u32(&mut cur)? as usize;
    take_bytes(&mut cur, len)
}

/// Encode a SCAN OK response payload: `item_count(u32) | [key_len(u32)|key|value_len(u32)|value]*`.
pub fn encode_scan_response(items: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(items.len() as u32).to_le_bytes());
    for (key, value) in items {
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    }
    out
}

/// Scan result items: a list of (key, value) pairs.
pub type ScanItems = Vec<(Vec<u8>, Vec<u8>)>;

/// Decode a SCAN OK response payload.
pub fn decode_scan_response(data: &[u8]) -> Result<ScanItems, String> {
    let mut cur = data;
    let count = take_u32(&mut cur)? as usize;
    if count > 1_000_000 {
        return Err(format!("suspiciously large scan result count: {count}"));
    }
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let kl = take_u32(&mut cur)? as usize;
        let k = take_bytes(&mut cur, kl)?;
        let vl = take_u32(&mut cur)? as usize;
        let v = take_bytes(&mut cur, vl)?;
        items.push((k, v));
    }
    Ok(items)
}

/// Encode an error string as a response payload: `msg_len(u32) | msg_bytes`.
pub fn encode_error_payload(msg: &str) -> Vec<u8> {
    let bytes = msg.as_bytes();
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
    out
}

/// Decode an error response payload.
pub fn decode_error_payload(data: &[u8]) -> Result<String, String> {
    let mut cur = data;
    let len = take_u32(&mut cur)? as usize;
    let bytes = take_bytes(&mut cur, len)?;
    String::from_utf8(bytes).map_err(|e| format!("invalid UTF-8 in error payload: {e}"))
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use kaya_raft::{AppendRequest, LogEntry};

    #[test]
    fn round_trip_vote_request() {
        let env = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::VoteRequest(VoteRequest {
                term: Term(3),
                candidate_id: NodeId(1),
                last_log_index: LogIndex(10),
                last_log_term: Term(2),
            }),
        };
        let encoded = encode_envelope(&env);
        // 4-byte length prefix is stripped before decode
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        assert_eq!(decoded.from, NodeId(1));
        assert_eq!(decoded.to, NodeId(2));
        if let Message::VoteRequest(vr) = decoded.message {
            assert_eq!(vr.term, Term(3));
            assert_eq!(vr.last_log_index, LogIndex(10));
        } else {
            panic!("wrong message type");
        }
    }

    #[test]
    fn round_trip_vote_response() {
        let env = Envelope {
            from: NodeId(2),
            to: NodeId(1),
            message: Message::VoteResponse(VoteResponse {
                term: Term(3),
                vote_granted: true,
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::VoteResponse(vr) = decoded.message {
            assert!(vr.vote_granted);
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn round_trip_append_request_with_entries() {
        let entries = vec![
            LogEntry {
                term: Term(1),
                command: b"put:hello:world".to_vec(),
            },
            LogEntry {
                term: Term(2),
                command: vec![],
            },
        ];
        let env = Envelope {
            from: NodeId(1),
            to: NodeId(3),
            message: Message::AppendRequest(AppendRequest {
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex(0),
                prev_log_term: Term(0),
                entries: entries.clone(),
                leader_commit: LogIndex(1),
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::AppendRequest(ar) = decoded.message {
            assert_eq!(ar.entries.len(), 2);
            assert_eq!(ar.entries[0].command, b"put:hello:world".to_vec());
            assert!(ar.entries[1].command.is_empty());
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn round_trip_install_snapshot_request() {
        use kaya_raft::{InstallSnapshotRequest, Term};

        let data = b"snapshot-bytes-v1".to_vec();
        let env = Envelope {
            from: NodeId(1),
            to: NodeId(3),
            message: Message::InstallSnapshotRequest(InstallSnapshotRequest {
                term: Term(4),
                leader_id: NodeId(1),
                last_included_index: LogIndex(64),
                last_included_term: Term(3),
                data: data.clone(),
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::InstallSnapshotRequest(req) = decoded.message {
            assert_eq!(req.last_included_index, LogIndex(64));
            assert_eq!(req.data, data);
        } else {
            panic!("wrong message type");
        }
    }

    #[test]
    fn round_trip_install_snapshot_response() {
        use kaya_raft::InstallSnapshotResponse;

        let env = Envelope {
            from: NodeId(3),
            to: NodeId(1),
            message: Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: Term(4),
                success: true,
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::InstallSnapshotResponse(resp) = decoded.message {
            assert!(resp.success);
        } else {
            panic!("wrong message type");
        }
    }

    #[test]
    fn round_trip_config_change_request() {
        use kaya_raft::ConfigChangeRequest;

        let env = Envelope {
            from: NodeId(1),
            to: NodeId(2),
            message: Message::ConfigChangeRequest(ConfigChangeRequest {
                term: Term(7),
                old_peers: vec![NodeId(1), NodeId(2)],
                new_peers: vec![NodeId(1), NodeId(2), NodeId(3)],
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::ConfigChangeRequest(req) = decoded.message {
            assert_eq!(req.term, Term(7));
            assert_eq!(req.old_peers.len(), 2);
            assert_eq!(req.new_peers.len(), 3);
        } else {
            panic!("wrong message type");
        }
    }

    #[test]
    fn round_trip_config_change_response() {
        use kaya_raft::ConfigChangeResponse;

        let env = Envelope {
            from: NodeId(2),
            to: NodeId(1),
            message: Message::ConfigChangeResponse(ConfigChangeResponse {
                term: Term(7),
                success: true,
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::ConfigChangeResponse(resp) = decoded.message {
            assert!(resp.success);
        } else {
            panic!("wrong message type");
        }
    }

    #[test]
    fn round_trip_append_response() {
        let env = Envelope {
            from: NodeId(2),
            to: NodeId(1),
            message: Message::AppendResponse(AppendResponse {
                term: Term(2),
                success: true,
                match_index: LogIndex(5),
            }),
        };
        let encoded = encode_envelope(&env);
        let decoded = decode_envelope(&encoded[4..]).unwrap();
        if let Message::AppendResponse(ar) = decoded.message {
            assert!(ar.success);
            assert_eq!(ar.match_index, LogIndex(5));
        } else {
            panic!("wrong type");
        }
    }

    #[test]
    fn round_trip_remove_member_payload() {
        use super::{decode_remove_member_payload, encode_remove_member_payload};
        let payload = encode_remove_member_payload(42);
        assert_eq!(decode_remove_member_payload(&payload).unwrap(), 42);
    }

    #[test]
    fn round_trip_put_payload() {
        let payload = encode_put_payload(b"mykey", b"myvalue");
        let (k, v) = decode_put_payload(&payload).unwrap();
        assert_eq!(k, b"mykey");
        assert_eq!(v, b"myvalue");
    }

    #[test]
    fn round_trip_scan_response() {
        let items = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ];
        let encoded = encode_scan_response(&items);
        let decoded = decode_scan_response(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    // ── malformed client payload tests ─────────────────────────────────────────

    #[test]
    fn decode_put_payload_empty() {
        assert!(decode_put_payload(&[]).is_err());
    }

    #[test]
    fn decode_put_payload_truncated_header() {
        assert!(decode_put_payload(&[0x05, 0x00]).is_err());
    }

    #[test]
    fn decode_put_payload_truncated_key() {
        let mut data = Vec::new();
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"short");
        assert!(decode_put_payload(&data).is_err());
    }

    #[test]
    fn decode_put_payload_truncated_value() {
        let mut data = Vec::new();
        data.extend_from_slice(&3u32.to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(b"key");
        data.extend_from_slice(b"short");
        assert!(decode_put_payload(&data).is_err());
    }

    #[test]
    fn decode_key_payload_empty() {
        assert!(decode_key_payload(&[]).is_err());
    }

    #[test]
    fn decode_key_payload_truncated_length() {
        assert!(decode_key_payload(&[0x01]).is_err());
    }

    #[test]
    fn decode_key_payload_truncated_data() {
        let mut data = Vec::new();
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(b"tiny");
        assert!(decode_key_payload(&data).is_err());
    }

    #[test]
    fn decode_value_payload_empty() {
        assert!(decode_value_payload(&[]).is_err());
    }

    #[test]
    fn decode_value_payload_truncated() {
        let mut data = Vec::new();
        data.extend_from_slice(&50u32.to_le_bytes());
        data.extend_from_slice(b"short");
        assert!(decode_value_payload(&data).is_err());
    }

    #[test]
    fn decode_scan_response_empty() {
        assert!(decode_scan_response(&[]).is_err());
    }

    #[test]
    fn decode_scan_response_oversized_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&2_000_000u32.to_le_bytes());
        assert!(decode_scan_response(&data).is_err());
    }

    #[test]
    fn decode_scan_response_truncated_items() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&5u32.to_le_bytes());
        data.extend_from_slice(b"key");
        assert!(decode_scan_response(&data).is_err());
    }

    #[test]
    fn decode_error_payload_empty() {
        assert!(decode_error_payload(&[]).is_err());
    }

    #[test]
    fn decode_error_payload_truncated() {
        let mut data = Vec::new();
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(b"err");
        assert!(decode_error_payload(&data).is_err());
    }

    #[test]
    fn decode_error_payload_invalid_utf8() {
        let mut data = Vec::new();
        let bad = vec![0xff, 0xfe, 0xfd];
        data.extend_from_slice(&(bad.len() as u32).to_le_bytes());
        data.extend_from_slice(&bad);
        assert!(decode_error_payload(&data).is_err());
    }

    // ── malformed Raft envelope tests ────────────────────────────────────────

    #[test]
    fn decode_envelope_empty() {
        assert!(decode_envelope(&[]).is_err());
    }

    #[test]
    fn decode_envelope_truncated_from() {
        assert!(decode_envelope(&[0x01, 0x02]).is_err());
    }

    #[test]
    fn decode_envelope_truncated_msg_type() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_unknown_msg_type() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(99);
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_vote_request_truncated() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(MSG_VOTE_REQUEST);
        data.extend_from_slice(&5u64.to_le_bytes());
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_append_request_oversized_entry_count() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(MSG_APPEND_REQUEST);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&200_000u32.to_le_bytes());
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_append_request_oversized_command() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(MSG_APPEND_REQUEST);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&(17 * 1024 * 1024u32).to_le_bytes());
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_append_request_truncated_entry() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(MSG_APPEND_REQUEST);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&0u64.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(b"short");
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_vote_response_truncated() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(MSG_VOTE_RESPONSE);
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn decode_envelope_append_response_truncated() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&2u64.to_le_bytes());
        data.push(MSG_APPEND_RESPONSE);
        data.extend_from_slice(&1u64.to_le_bytes());
        assert!(decode_envelope(&data).is_err());
    }

    #[test]
    fn all_decoders_no_panic_on_garbage() {
        let cases: &[&[u8]] = &[
            &[0xff],
            &[0x00, 0x00, 0x00, 0xff],
            &[0xff; 64],
            &[0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff],
        ];
        for input in cases {
            let _ = decode_put_payload(input);
            let _ = decode_key_payload(input);
            let _ = decode_value_payload(input);
            let _ = decode_scan_response(input);
            let _ = decode_error_payload(input);
            let _ = decode_envelope(input);
        }
    }
}
