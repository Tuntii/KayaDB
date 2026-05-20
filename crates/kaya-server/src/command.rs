//! [`RaftCommand`]: the write operations that flow through the Raft log.
//!
//! Every `put` or `delete` that a client issues is encoded as a `RaftCommand`,
//! appended to the Raft log on the leader, replicated to a quorum, and then
//! decoded and applied to the local [`Engine`] once committed.
//!
//! Wire format:
//!
//! ```text
//! type      : u8       (1 = Put, 2 = Delete)
//! key_len   : u32 LE
//! key       : bytes
//! [Put only]
//! value_len : u32 LE
//! value     : bytes
//! ```

/// A replicated write command stored in the Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftCommand {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl RaftCommand {
    /// Encode this command to bytes for storage in a Raft log entry.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            RaftCommand::Put { key, value } => {
                out.push(1u8);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
            RaftCommand::Delete { key } => {
                out.push(2u8);
                out.extend_from_slice(&(key.len() as u32).to_le_bytes());
                out.extend_from_slice(key);
            }
        }
        out
    }

    /// Decode a command from bytes.
    pub fn decode(data: &[u8]) -> Result<Self, String> {
        let mut cur = data;
        let cmd_type = next_u8(&mut cur)?;
        match cmd_type {
            1 => {
                let key = next_bytes(&mut cur)?;
                let value = next_bytes(&mut cur)?;
                Ok(RaftCommand::Put { key, value })
            }
            2 => {
                let key = next_bytes(&mut cur)?;
                Ok(RaftCommand::Delete { key })
            }
            t => Err(format!("unknown RaftCommand type: {t}")),
        }
    }
}

// ── mini parsing helpers ──────────────────────────────────────────────────────

fn next_u8(cur: &mut &[u8]) -> Result<u8, String> {
    if cur.is_empty() {
        return Err("unexpected EOF reading u8".to_owned());
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn next_u32(cur: &mut &[u8]) -> Result<u32, String> {
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

fn next_bytes(cur: &mut &[u8]) -> Result<Vec<u8>, String> {
    let len = next_u32(cur)? as usize;
    if cur.len() < len {
        return Err(format!(
            "truncated data: need {len} bytes, have {}",
            cur.len()
        ));
    }
    let bytes = cur[..len].to_vec();
    *cur = &cur[len..];
    Ok(bytes)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_put() {
        let cmd = RaftCommand::Put {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn round_trip_delete() {
        let cmd = RaftCommand::Delete {
            key: b"some_key".to_vec(),
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn put_with_empty_value() {
        let cmd = RaftCommand::Put {
            key: b"k".to_vec(),
            value: vec![],
        };
        let encoded = cmd.encode();
        assert_eq!(RaftCommand::decode(&encoded).unwrap(), cmd);
    }

    #[test]
    fn unknown_type_is_error() {
        let bad = vec![99u8]; // unknown type
        assert!(RaftCommand::decode(&bad).is_err());
    }
}
